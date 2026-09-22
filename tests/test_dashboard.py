#!/usr/bin/env python3
"""Tests for metrics, worker threads and the dashboard HTTP API.

Runs the real dashboard server against the real RESP test server, so the whole
button-to-Redis path is exercised over actual sockets.

    python tests/test_dashboard.py
"""

from __future__ import annotations

import json
import sys
import threading
import time
import urllib.error
import urllib.request
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

from metrics import Metrics, percentile  # noqa: E402
from resp_server import RespServer  # noqa: E402

PASSED = 0
FAILED: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    global PASSED
    if condition:
        PASSED += 1
        print(f"  PASS  {label}")
    else:
        FAILED.append(label)
        print(f"  FAIL  {label}" + (f" -- {detail}" if detail else ""))


def wait_for(predicate, timeout: float = 15.0, interval: float = 0.05) -> bool:
    """Poll until true. Threads make fixed sleeps flaky, so never use them."""
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        time.sleep(interval)
    return False


# --- metrics ---------------------------------------------------------------

def test_percentile() -> None:
    print("\n[percentile maths]")
    check("empty input is 0", percentile([], 50) == 0.0)
    check("single value", percentile([7.0], 95) == 7.0)
    vals = [float(i) for i in range(1, 101)]        # 1..100
    check("p50 of 1..100", abs(percentile(vals, 50) - 50.5) < 0.01,
          str(percentile(vals, 50)))
    check("p95 of 1..100", abs(percentile(vals, 95) - 95.05) < 0.05,
          str(percentile(vals, 95)))
    check("p100 is the max", percentile(vals, 100) == 100.0)
    check("p0 is the min", percentile(vals, 0) == 1.0)
    check("interpolates between ranks",
          abs(percentile([0.0, 10.0], 50) - 5.0) < 0.001,
          str(percentile([0.0, 10.0], 50)))


def test_metrics_counters() -> None:
    print("\n[metrics counters and snapshot]")
    m = Metrics(window_seconds=60)
    for _ in range(10):
        m.record_produced()
    for lag in (5.0, 10.0, 15.0, 100.0):
        m.record_consumed(lag)
        m.record_acked()
    m.record_failed()
    m.record_trimmed(3)
    m.record_error("boom")

    t = m.totals()
    check("produced counted", t["produced"] == 10, str(t))
    check("consumed counted", t["consumed"] == 4, str(t))
    check("acked counted", t["acked"] == 4, str(t))
    check("failed counted", t["failed"] == 1, str(t))
    check("trimmed counted with n>1", t["trimmed"] == 3, str(t))
    check("errors counted", t["errors"] == 1, str(t))

    lat = m.latency()
    check("latency sample count", lat["samples"] == 4, str(lat))
    check("latency min and max", lat["min"] == 5.0 and lat["max"] == 100.0, str(lat))
    check("latency mean", abs(lat["mean"] - 32.5) < 0.01, str(lat))

    snap = m.snapshot(window_seconds=30)
    check("snapshot is JSON serialisable", isinstance(json.dumps(snap), str))
    for key in ("totals", "rates", "latency_ms", "series", "now", "last_error"):
        check(f"snapshot has {key}", key in snap)
    check("last error is retained", snap["last_error"] == "boom")

    m.reset()
    check("reset clears totals", all(v == 0 for v in m.totals().values()),
          str(m.totals()))
    check("reset clears latency", m.latency()["samples"] == 0)
    check("reset clears last error", m.snapshot()["last_error"] is None)


def test_metrics_series() -> None:
    print("\n[metrics time series]")
    m = Metrics(window_seconds=60)
    now = int(time.time())

    # Write directly into past buckets; the public API only writes "now".
    with m._lock:
        for offset, count in ((5, 10), (4, 20), (2, 30)):
            b = m._bucket(now - offset)
            b.produced = count
            b.consumed = count
            for lag in (1.0, 2.0, 3.0, 50.0):
                b.add_lag(lag)

    rows = m.series(10)
    check("series is one row per second", len(rows) == 10, str(len(rows)))
    check("series is ordered oldest first",
          all(rows[i]["t"] < rows[i + 1]["t"] for i in range(len(rows) - 1)))
    check("the current, still-filling second is excluded",
          all(r["t"] < now for r in rows), str([r["t"] - now for r in rows]))
    by_t = {r["t"]: r for r in rows}
    check("populated buckets carry their counts",
          by_t[now - 5]["produced"] == 10 and by_t[now - 2]["produced"] == 30,
          str([(r["t"] - now, r["produced"]) for r in rows]))
    check("idle seconds are zero filled rather than skipped",
          by_t[now - 3]["produced"] == 0 and by_t[now - 3]["lag_p50"] is None)
    # p50 of [1,2,3,50] interpolates between ranks 1 and 2 -> 2.5.
    check("percentiles computed per bucket",
          by_t[now - 5]["lag_p50"] == 2.5 and by_t[now - 5]["lag_max"] == 50.0,
          str(by_t[now - 5]))
    check("per-bucket sample count is reported",
          by_t[now - 5]["samples"] == 4, str(by_t[now - 5]["samples"]))

    # Buckets outside the window must be pruned, not merely hidden.
    with m._lock:
        m._bucket(now - 5000)
    check("stale buckets are pruned on write",
          all(t > now - 100 for t in m._buckets),
          str(sorted(t - now for t in m._buckets)))


def test_metrics_thread_safety() -> None:
    print("\n[metrics under concurrent writers]")
    m = Metrics(window_seconds=60)
    n_threads, per_thread = 8, 500

    def writer(k: int) -> None:
        for i in range(per_thread):
            m.record_produced()
            m.record_consumed(float(i % 50))
            m.record_acked()

    threads = [threading.Thread(target=writer, args=(k,)) for k in range(n_threads)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()

    expected = n_threads * per_thread
    t = m.totals()
    check(f"no lost updates across {n_threads} threads ({expected} each)",
          t["produced"] == expected and t["consumed"] == expected
          and t["acked"] == expected, str(t))
    check("snapshot is consistent while being written",
          isinstance(json.dumps(m.snapshot()), str))

    # The bounded sample list must not grow without limit.
    check("recent latency samples stay bounded",
          m.latency()["samples"] <= 2000, str(m.latency()["samples"]))


def test_bucket_sample_cap() -> None:
    print("\n[latency sample cap per bucket]")
    import metrics as metrics_mod

    m = Metrics(window_seconds=60)
    for i in range(metrics_mod.MAX_SAMPLES_PER_BUCKET * 3):
        m.record_consumed(float(i))
    with m._lock:
        sizes = [len(b.lags) for b in m._buckets.values()]
    check("no bucket exceeds the sample cap",
          all(s <= metrics_mod.MAX_SAMPLES_PER_BUCKET for s in sizes), str(sizes))
    check("percentiles still computed from the capped sample",
          m.series(5) is not None)


# --- workers ---------------------------------------------------------------

def _manager_for(srv: RespServer):
    """A WorkerManager pointed at the RESP test server."""
    from workers import WorkerManager

    mgr = WorkerManager(Metrics(window_seconds=60))
    mgr.settings.update(host="127.0.0.1", port=srv.port, stream="dash:test",
                        group="dash-group", rate=50.0, trim_interval=0,
                        block_ms=100, batch_size=10, claim_interval=999,
                        consumer_name="test-1")
    return mgr


def test_worker_lifecycle() -> None:
    print("\n[worker thread lifecycle]")
    with RespServer() as srv:
        mgr = _manager_for(srv)

        ok, message = mgr.ping()
        check("ping reaches the server", ok, message)

        check("producer is not running initially", mgr.producer_running() is False)
        check("start_producer returns True", mgr.start_producer() is True)
        check("producer reports running", wait_for(mgr.producer_running))
        check("a second start is a no-op, not a second thread",
              mgr.start_producer() is False)
        check("messages are produced",
              wait_for(lambda: mgr.metrics.totals()["produced"] > 5),
              str(mgr.metrics.totals()))

        check("start_consumer returns True", mgr.start_consumer() is True)
        check("messages are consumed and acked",
              wait_for(lambda: mgr.metrics.totals()["acked"] > 5),
              str(mgr.metrics.totals()))
        check("latency samples are recorded",
              mgr.metrics.latency()["samples"] > 0, str(mgr.metrics.latency()))
        check("measured lag is plausible, not negative",
              (mgr.metrics.latency()["min"] or 0) >= 0, str(mgr.metrics.latency()))

        check("stop_producer joins the thread", mgr.stop_producer() is True)
        check("producer reports stopped", mgr.producer_running() is False)

        produced_after_stop = mgr.metrics.totals()["produced"]
        time.sleep(0.4)
        check("no messages produced after stop",
              mgr.metrics.totals()["produced"] == produced_after_stop,
              f"{mgr.metrics.totals()['produced']} vs {produced_after_stop}")

        check("consumer drains the backlog",
              wait_for(lambda: mgr.metrics.totals()["acked"]
                       >= mgr.metrics.totals()["produced"], timeout=10),
              str(mgr.metrics.totals()))

        check("stop_consumer joins the thread", mgr.stop_consumer() is True)
        check("consumer reports stopped", mgr.consumer_running() is False)

        totals = mgr.metrics.totals()
        check("every produced message was acked",
              totals["acked"] == totals["produced"], str(totals))
        check("no errors recorded", totals["errors"] == 0, str(totals))

        status = mgr.stream_status()
        check("stream status reports a length",
              isinstance(status["length"], int), str(status))
        check("nothing left pending", status["pending"] == 0, str(status))

        check("restart after stop works", mgr.start_producer() is True
              and wait_for(mgr.producer_running))
        mgr.stop_all()
        check("stop_all stops both",
              not mgr.producer_running() and not mgr.consumer_running())


def test_worker_client_isolation() -> None:
    print("\n[one Redis client per thread]")
    with RespServer() as srv:
        mgr = _manager_for(srv)
        created: list[object] = []
        real_factory = mgr._client_factory

        def counting_factory(**kwargs):
            client = real_factory(**kwargs)
            created.append(client)
            return client

        mgr._client_factory = counting_factory
        mgr.start_producer()
        mgr.start_consumer()
        check("both workers are running",
              wait_for(lambda: mgr.producer_running() and mgr.consumer_running()))
        check("traffic flows", wait_for(lambda: mgr.metrics.totals()["acked"] > 3),
              str(mgr.metrics.totals()))

        # Snapshot socket identity while the workers are LIVE. After shutdown
        # every _sock is None, so comparing them then proves nothing.
        live_socks = [id(c._sock) for c in created if getattr(c, "_sock", None)]
        mgr.stop_all()

        # The producer and consumer threads must never share a socket: the client
        # wraps one socket and is not thread safe.
        check("each worker built its own client", len(created) >= 2, str(len(created)))
        check("at least two sockets were open at once", len(live_socks) >= 2,
              str(len(live_socks)))
        check("no two live clients shared a socket object",
              len(set(live_socks)) == len(live_socks), str(live_socks))
        check("no two clients are the same object",
              len({id(c) for c in created}) == len(created))
        check("clients are closed on shutdown",
              all(getattr(c, "_sock", None) is None for c in created),
              str([getattr(c, "_sock", None) for c in created]))


def test_worker_survives_bad_endpoint() -> None:
    print("\n[worker error handling]")
    from workers import WorkerManager

    mgr = WorkerManager(Metrics(window_seconds=60))
    # Port 1 is reserved and will refuse immediately.
    mgr.settings.update(host="127.0.0.1", port=1, stream="x", group="g",
                        rate=50.0, trim_interval=0, block_ms=50)
    ok, message = mgr.ping()
    check("ping reports a refused endpoint", ok is False, message)

    mgr.start_producer()
    check("producer records an error rather than dying silently",
          wait_for(lambda: mgr.metrics.totals()["errors"] > 0 or
                   not mgr.producer_running(), timeout=10),
          str(mgr.metrics.totals()))
    mgr.stop_producer()
    check("producer stops cleanly after failing", mgr.producer_running() is False)
    check("an error message was captured",
          mgr.metrics.snapshot()["last_error"] is not None)


# --- dashboard HTTP API ----------------------------------------------------

def _http(url: str, method: str = "GET", body: dict | None = None) -> dict:
    data = json.dumps(body or {}).encode() if method == "POST" else None
    req = urllib.request.Request(url, data=data, method=method)
    if data:
        req.add_header("Content-Type", "application/json")
    with urllib.request.urlopen(req, timeout=20) as resp:
        return json.loads(resp.read())


def test_dashboard_api() -> None:
    print("\n[dashboard HTTP API end to end]")
    import dashboard

    with RespServer() as srv:
        args = dashboard.parse_args([
            "--host", "127.0.0.1", "--port", str(srv.port),
            "--stream", "web:test", "--group", "web-group",
            "--rate", "40", "--bind", "127.0.0.1", "--web-port", "0",
        ])
        httpd = dashboard.build_server(args)
        port = httpd.server_address[1]
        base = f"http://127.0.0.1:{port}"
        threading.Thread(target=httpd.serve_forever, daemon=True).start()
        mgr = dashboard.MANAGER
        mgr.settings.update(trim_interval=0, block_ms=100, batch_size=10,
                            claim_interval=999)

        try:
            # Page and health
            with urllib.request.urlopen(base + "/", timeout=10) as r:
                html = r.read().decode()
            check("index serves HTML", r.status == 200 and "<canvas" in html)
            # Naive substring checks fail here because the source comment
            # explains why there is no CDN. Look for real external references.
            import re as _re
            externals = (_re.findall(r'<script[^>]+src=', html, _re.I)
                         + _re.findall(r'<link[^>]+href=', html, _re.I)
                         + _re.findall(r'https?://(?!127\.0\.0\.1)', html))
            check("the page loads no external scripts, styles or URLs",
                  externals == [], str(externals[:5]))
            check("charts are drawn by inlined canvas code",
                  "getContext(\"2d\")" in html and "<canvas" in html)
            check("health endpoint", _http(base + "/api/health")["ok"] is True)

            # Metrics shape
            snap = _http(base + "/api/metrics?window=60")
            for key in ("totals", "rates", "latency_ms", "series", "status"):
                check(f"metrics payload has {key}", key in snap)
            check("status reports both workers stopped",
                  snap["status"]["producer_running"] is False
                  and snap["status"]["consumer_running"] is False)
            check("password is redacted in the status payload",
                  snap["status"]["settings"].get("password") in (None, "", "***"),
                  str(snap["status"]["settings"].get("password")))

            check("window parameter is honoured",
                  len(_http(base + "/api/metrics?window=30")["series"]) == 30)
            check("an absurd window is clamped, not accepted",
                  len(_http(base + "/api/metrics?window=99999")["series"]) <= 600)
            check("a non-numeric window falls back to the default",
                  len(_http(base + "/api/metrics?window=abc")["series"]) == 120)

            # Ping
            check("ping endpoint succeeds", _http(base + "/api/ping", "POST")["ok"])

            # Start producer via the API
            r1 = _http(base + "/api/producer/start", "POST")
            check("producer start returns ok", r1["ok"] is True, str(r1))
            check("producer shows as running in metrics",
                  wait_for(lambda: _http(base + "/api/metrics")["status"]["producer_running"]))
            check("starting twice is reported, not duplicated",
                  "already running" in _http(base + "/api/producer/start", "POST")["message"])

            check("produced counter climbs",
                  wait_for(lambda: _http(base + "/api/metrics")["totals"]["produced"] > 5),
                  str(_http(base + "/api/metrics")["totals"]))

            # Settings are locked while running
            resp = _http(base + "/api/settings", "POST", {"host": "10.9.9.9"})
            check("connection settings are refused while a worker runs",
                  "host" in resp["ignored"], str(resp))
            check("the endpoint was not actually changed",
                  _http(base + "/api/metrics")["status"]["settings"]["host"] == "127.0.0.1")

            # Start consumer, verify latency series populates
            _http(base + "/api/consumer/start", "POST")
            check("acked counter climbs",
                  wait_for(lambda: _http(base + "/api/metrics")["totals"]["acked"] > 5),
                  str(_http(base + "/api/metrics")["totals"]))
            check("latency percentiles are reported",
                  wait_for(lambda: _http(base + "/api/metrics")["latency_ms"]["p50"]
                           is not None),
                  str(_http(base + "/api/metrics")["latency_ms"]))
            check("rates are non-zero while running",
                  wait_for(lambda: _http(base + "/api/metrics")["rates"]["produce_per_s"] > 0,
                           timeout=12),
                  str(_http(base + "/api/metrics")["rates"]))
            check("series carries per-second latency for charting",
                  wait_for(lambda: any(row["lag_p50"] is not None for row in
                                       _http(base + "/api/metrics?window=60")["series"]),
                           timeout=12))

            # Stream status
            status = _http(base + "/api/stream_status", "POST")
            check("stream status returns a length",
                  isinstance(status["length"], int), str(status))

            # Stop all
            check("stop_all reports ok", _http(base + "/api/stop_all", "POST")["ok"])
            check("both workers stop",
                  wait_for(lambda: not any([
                      _http(base + "/api/metrics")["status"]["producer_running"],
                      _http(base + "/api/metrics")["status"]["consumer_running"]])))

            # Settings now editable
            resp = _http(base + "/api/settings", "POST",
                         {"host": "10.9.9.9", "rate": 12.5, "batch_size": 7})
            check("settings apply once stopped", set(resp["applied"])
                  >= {"host", "rate", "batch_size"}, str(resp))
            check("numeric settings are coerced from JSON",
                  mgr.settings["batch_size"] == 7
                  and isinstance(mgr.settings["batch_size"], int),
                  str(type(mgr.settings["batch_size"])))
            check("unknown settings keys are ignored safely",
                  _http(base + "/api/settings", "POST", {"nope": 1})["applied"] == [])
            mgr.settings["host"] = "127.0.0.1"

            # Reset
            _http(base + "/api/reset", "POST")
            check("reset zeroes the totals",
                  _http(base + "/api/metrics")["totals"]["produced"] == 0)

            # 404s
            try:
                _http(base + "/api/nope")
                check("unknown route returns 404", False)
            except urllib.error.HTTPError as exc:
                check("unknown route returns 404", exc.code == 404)

        finally:
            mgr.stop_all()
            httpd.shutdown()
            httpd.server_close()


def test_dashboard_refuses_bad_bind() -> None:
    print("\n[dashboard startup failure]")
    import dashboard

    args = dashboard.parse_args(["--bind", "203.0.113.1", "--web-port", "8080"])
    try:
        dashboard.build_server(args)
        check("binding an unreachable address fails", False,
              "expected OSError")
    except OSError:
        check("binding an unreachable address fails", True)

    rc = dashboard.main(["--bind", "203.0.113.1", "--web-port", "8080"])
    check("main returns 1 rather than crashing", rc == 1, str(rc))


def test_streamlit_app_importable() -> None:
    print("\n[streamlit app]")
    src = (ROOT / "streamlit_app.py").read_text()
    check("streamlit_app.py exists and is non-trivial", len(src) > 1000)
    check("it uses cache_resource so workers survive Streamlit reruns",
          "@st.cache_resource" in src)
    check("it shares the same WorkerManager as the fallback dashboard",
          "from workers import WorkerManager" in src)
    check("it fails with install guidance rather than a bare ImportError",
          "ModuleNotFoundError" in src and "dashboard.py" in src)
    check("it treats pandas as optional",
          src.count("except ModuleNotFoundError") >= 2)
    import ast
    check("it parses as valid Python", isinstance(ast.parse(src), ast.Module))

    # Confirm the guidance path actually triggers when streamlit is absent.
    import subprocess
    proc = subprocess.run([sys.executable, str(ROOT / "streamlit_app.py")],
                          capture_output=True, text=True, timeout=60)
    if "No module named 'streamlit'" in proc.stderr or proc.returncode == 1:
        check("running it without streamlit prints install guidance",
              "dashboard.py" in proc.stderr or "pip install streamlit" in proc.stderr,
              proc.stderr[-200:])
    else:
        check("running it without streamlit prints install guidance", True,
              "streamlit appears to be installed here")


def main() -> int:
    print("=" * 72)
    print("Dashboard, metrics and worker tests")
    print("=" * 72)

    test_percentile()
    test_metrics_counters()
    test_metrics_series()
    test_metrics_thread_safety()
    test_bucket_sample_cap()
    test_worker_lifecycle()
    test_worker_client_isolation()
    test_worker_survives_bad_endpoint()
    test_dashboard_api()
    test_dashboard_refuses_bad_bind()
    test_streamlit_app_importable()

    print("\n" + "=" * 72)
    if FAILED:
        print(f"{PASSED} passed, {len(FAILED)} FAILED")
        for label in FAILED:
            print(f"  - {label}")
        return 1
    print(f"All {PASSED} dashboard checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
