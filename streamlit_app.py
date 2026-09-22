#!/usr/bin/env python3
"""Streamlit control panel and performance dashboard for the Redis stream service.

    pip install streamlit          # needs wheels; see fetch_wheels.py for airgapped hosts
    streamlit run streamlit_app.py

If Streamlit cannot be installed on the target host, `dashboard.py` gives the
same buttons and charts with no dependencies at all.

Start/stop toggles run the producer and consumer as background threads inside
this process (see workers.py), so the charts read from the same in-memory metrics
the workers write to. Nothing is polled out of Redis to draw them.
"""

from __future__ import annotations

import sys
import time

try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - guidance, not logic
    sys.stderr.write(
        "\nStreamlit is not installed.\n\n"
        "  With internet:      pip install streamlit\n"
        "  Airgapped host:     python fetch_wheels.py   (on a connected machine)\n"
        "                      pip install --no-index --find-links wheels streamlit\n"
        "  No install at all:  python dashboard.py      (zero dependency alternative)\n\n")
    raise SystemExit(1)

from metrics import Metrics          # noqa: E402
from workers import WorkerManager    # noqa: E402

REFRESH_SECONDS = 2

st.set_page_config(page_title="Redis stream service", page_icon="•",
                   layout="wide")


# --- One manager for the whole session ------------------------------------

@st.cache_resource
def get_manager() -> WorkerManager:
    """Streamlit reruns this script top to bottom on every interaction.

    cache_resource keeps a single WorkerManager (and therefore a single set of
    worker threads and one Metrics instance) alive across those reruns. Without
    it, every button click would start a fresh manager and orphan the threads.
    """
    return WorkerManager(Metrics(window_seconds=600))


mgr = get_manager()


# --- Sidebar: connection and tuning ---------------------------------------

with st.sidebar:
    st.header("Connection")
    busy = mgr.producer_running() or mgr.consumer_running()
    if busy:
        st.caption("Stop both workers to change connection settings.")

    s = mgr.settings
    s["host"] = st.text_input("Host", s["host"], disabled=busy)
    s["port"] = st.number_input("Port", 1, 65535, int(s["port"]), disabled=busy)
    s["db"] = st.number_input("DB", 0, 15, int(s["db"]), disabled=busy)
    s["username"] = st.text_input("Username", s["username"] or "",
                                  disabled=busy) or None
    s["password"] = st.text_input("Password", s["password"] or "", type="password",
                                  disabled=busy) or None
    s["ssl"] = st.checkbox("Use TLS", bool(s["ssl"]), disabled=busy)
    if s["ssl"]:
        s["ssl_verify"] = not st.checkbox(
            "Accept self-signed certificate", not bool(s["ssl_verify"]),
            disabled=busy,
            help="The PoC cluster uses a self-signed certificate.")

    if st.button("Test connection", use_container_width=True):
        ok, message = mgr.ping()
        (st.success if ok else st.error)(message)

    st.divider()
    st.header("Stream")
    s["stream"] = st.text_input("Stream key", s["stream"], disabled=busy)
    s["group"] = st.text_input("Consumer group", s["group"], disabled=busy)
    s["consumer_name"] = st.text_input("Consumer name", s["consumer_name"],
                                       disabled=busy)

    st.divider()
    st.header("Producer")
    s["rate"] = st.slider("Rate (msg/s)", 0.5, 200.0, float(s["rate"]), 0.5,
                          disabled=mgr.producer_running(),
                          help="Applied when the producer starts.")
    s["ttl_seconds"] = st.number_input("TTL (seconds)", 10, 86400,
                                       int(s["ttl_seconds"]),
                                       disabled=mgr.producer_running(),
                                       help="Enforced with XTRIM MINID.")
    s["trim_interval"] = st.number_input("Trim interval (s)", 1, 300,
                                         int(s["trim_interval"]),
                                         disabled=mgr.producer_running())

    st.divider()
    st.header("Consumer")
    s["batch_size"] = st.number_input("Batch size (COUNT)", 1, 1000,
                                      int(s["batch_size"]),
                                      disabled=mgr.consumer_running())
    s["block_ms"] = st.number_input("Block (ms)", 100, 60000, int(s["block_ms"]),
                                    disabled=mgr.consumer_running())

    st.divider()
    auto = st.checkbox("Auto refresh", True)
    window = st.select_slider("Chart window",
                              options=[60, 120, 300, 600], value=120,
                              format_func=lambda v: f"{v // 60} min" if v >= 60 else f"{v}s")
    if st.button("Reset metrics", use_container_width=True):
        mgr.metrics.reset()
        st.toast("Metrics reset")


# --- Header and controls ---------------------------------------------------

st.title("Redis stream service")
st.caption(f"`{s['stream']}` on {s['host']}:{s['port']}  ·  group `{s['group']}`"
           f"  ·  TTL {s['ttl_seconds']}s via XTRIM MINID")

c1, c2, c3, c4 = st.columns([1, 1, 1, 2])

with c1:
    if mgr.producer_running():
        if st.button("Stop producer", type="primary", use_container_width=True):
            mgr.stop_producer()
            st.rerun()
    else:
        if st.button("Start producer", use_container_width=True):
            ok, message = mgr.ping()
            if ok:
                mgr.start_producer()
                st.rerun()
            else:
                st.error(f"Not starting: {message}")

with c2:
    if mgr.consumer_running():
        if st.button("Stop consumer", type="primary", use_container_width=True):
            mgr.stop_consumer()
            st.rerun()
    else:
        if st.button("Start consumer", use_container_width=True):
            ok, message = mgr.ping()
            if ok:
                mgr.start_consumer()
                st.rerun()
            else:
                st.error(f"Not starting: {message}")

with c3:
    if st.button("Stop all", use_container_width=True,
                 disabled=not (mgr.producer_running() or mgr.consumer_running())):
        mgr.stop_all()
        st.rerun()

with c4:
    prod = "running" if mgr.producer_running() else "stopped"
    cons = "running" if mgr.consumer_running() else "stopped"
    st.markdown(
        f"**Producer** {'🟢' if prod == 'running' else '⚪'} {prod} &nbsp;&nbsp; "
        f"**Consumer** {'🟢' if cons == 'running' else '⚪'} {cons}")

snap = mgr.metrics.snapshot(window_seconds=window)
if snap["last_error"]:
    st.warning(f"Last error: {snap['last_error']}")


# --- Summary cards ---------------------------------------------------------

totals, rates, lat = snap["totals"], snap["rates"], snap["latency_ms"]

m1, m2, m3, m4, m5, m6 = st.columns(6)
m1.metric("Produced", f"{totals['produced']:,}",
          f"{rates['produce_per_s']:.1f}/s")
m2.metric("Consumed", f"{totals['consumed']:,}",
          f"{rates['consume_per_s']:.1f}/s")
m3.metric("Acked", f"{totals['acked']:,}", f"{rates['ack_per_s']:.1f}/s")
m4.metric("Lag p50", f"{lat['p50']:.0f} ms" if lat["p50"] is not None else "-")
m5.metric("Lag p95", f"{lat['p95']:.0f} ms" if lat["p95"] is not None else "-")
m6.metric("Lag max", f"{lat['max']:.0f} ms" if lat["max"] is not None else "-")

backlog = totals["produced"] - totals["consumed"]
n1, n2, n3, n4 = st.columns(4)
n1.metric("In flight", f"{backlog:,}",
          help="Produced minus consumed since the metrics were last reset.")
n2.metric("Trimmed by TTL", f"{totals['trimmed']:,}")
n3.metric("Failed handles", f"{totals['failed']:,}")
n4.metric("Errors", f"{totals['errors']:,}")


# --- Charts ---------------------------------------------------------------

series = snap["series"]

if not series or all(r["produced"] == 0 and r["consumed"] == 0 for r in series):
    st.info("No traffic yet. Start the producer and consumer to populate the charts.")
else:
    # Build plain dicts keyed by a wall-clock label. Chart helpers accept a dict
    # of column -> values, so pandas is not required.
    import datetime as _dt

    labels = [_dt.datetime.fromtimestamp(r["t"]).strftime("%H:%M:%S") for r in series]

    st.subheader("Throughput")
    st.caption("Messages per second, one point per second. The current, "
               "still-filling second is excluded.")
    try:
        import pandas as pd

        tp = pd.DataFrame(
            {"produced/s": [r["produced"] for r in series],
             "consumed/s": [r["consumed"] for r in series]},
            index=pd.to_datetime([r["t"] for r in series], unit="s"))
        st.line_chart(tp, height=260)
    except ModuleNotFoundError:
        st.line_chart({"produced/s": [r["produced"] for r in series],
                       "consumed/s": [r["consumed"] for r in series]}, height=260)

    st.subheader("End-to-end latency")
    st.caption("Milliseconds from XADD to XACK, derived from the "
               "`produced_at_ms` field on each entry. Gaps mean no traffic.")
    has_lat = any(r["lag_p50"] is not None for r in series)
    if not has_lat:
        st.info("No latency samples yet. Start the consumer to measure lag.")
    else:
        try:
            import pandas as pd

            lt = pd.DataFrame(
                {"p50 ms": [r["lag_p50"] for r in series],
                 "p95 ms": [r["lag_p95"] for r in series],
                 "max ms": [r["lag_max"] for r in series]},
                index=pd.to_datetime([r["t"] for r in series], unit="s"))
            st.line_chart(lt, height=260)
        except ModuleNotFoundError:
            st.line_chart({"p50 ms": [r["lag_p50"] or 0 for r in series],
                           "p95 ms": [r["lag_p95"] or 0 for r in series],
                           "max ms": [r["lag_max"] or 0 for r in series]},
                          height=260)

    with st.expander("Per-second detail"):
        rows = [{"time": labels[i], **{k: v for k, v in r.items() if k != "t"}}
                for i, r in enumerate(series)]
        st.dataframe(list(reversed(rows)), use_container_width=True, height=300)


# --- Stream status (read on demand, not on every rerun) -------------------

with st.expander("Stream status from Redis"):
    if st.button("Read stream status"):
        status = mgr.stream_status()
        if status["error"]:
            st.error(status["error"])
        else:
            a, b = st.columns(2)
            a.metric("Stream length", f"{status['length']:,}"
                     if status["length"] is not None else "-",
                     help="Entries currently in the stream, after TTL trimming.")
            b.metric("Pending (unacked)", f"{status['pending']:,}"
                     if status["pending"] is not None else "group not created")
            if status["consumers"]:
                st.write("Consumers in the group:")
                st.dataframe(status["consumers"], use_container_width=True)
    else:
        st.caption("Reads XLEN and XPENDING. Kept behind a button so the "
                   "dashboard does not add Redis traffic on every refresh.")


# --- Auto refresh ----------------------------------------------------------

if auto and (mgr.producer_running() or mgr.consumer_running()):
    time.sleep(REFRESH_SECONDS)
    st.rerun()
elif auto:
    st.caption(f"Auto refresh pauses while both workers are stopped.")
