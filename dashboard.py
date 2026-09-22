#!/usr/bin/env python3
"""Zero-dependency control panel and performance dashboard.

Same buttons and charts as streamlit_app.py, but built on http.server with the
HTML, CSS and JavaScript inlined. No pip install, no CDN, no internet. Use this
on a host where Streamlit cannot be installed.

    python dashboard.py --host redis.example.com --port 12000
    # then open http://127.0.0.1:8080

The charts are drawn on a <canvas> by hand, because every charting library would
be either a pip install or a CDN fetch, and this bundle can have neither.
"""

from __future__ import annotations

import argparse
import json
import logging
import socket
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import config
from metrics import Metrics
from workers import WorkerManager

log = logging.getLogger("dashboard")

MANAGER: WorkerManager = WorkerManager(Metrics(window_seconds=600))


# --- Front end -------------------------------------------------------------

PAGE = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Redis stream service</title>
<meta name="viewport" content="width=device-width,initial-scale=1">
<style>
  :root{
    --bg:#0f1419; --panel:#171e26; --panel2:#1e2733; --line:#2b3948;
    --fg:#e6edf3; --dim:#8b9aa9; --red:#ff4438; --green:#3fb950;
    --blue:#58a6ff; --amber:#d29922;
  }
  *{box-sizing:border-box}
  body{margin:0;background:var(--bg);color:var(--fg);
    font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",Roboto,Helvetica,Arial,sans-serif}
  header{padding:18px 24px;border-bottom:1px solid var(--line);
    display:flex;align-items:center;gap:16px;flex-wrap:wrap}
  h1{font-size:18px;margin:0;font-weight:650}
  .sub{color:var(--dim);font-size:12px;font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
  .wrap{padding:20px 24px;max-width:1500px}
  .row{display:flex;gap:12px;flex-wrap:wrap;align-items:center;margin-bottom:18px}
  button{background:var(--panel2);color:var(--fg);border:1px solid var(--line);
    border-radius:6px;padding:9px 16px;font-size:13px;font-weight:550;cursor:pointer}
  button:hover:not(:disabled){border-color:var(--dim)}
  button:disabled{opacity:.4;cursor:not-allowed}
  button.on{background:var(--red);border-color:var(--red);color:#fff}
  button.ghost{background:transparent}
  .pill{display:inline-flex;align-items:center;gap:7px;font-size:12px;
    padding:6px 12px;border:1px solid var(--line);border-radius:999px;color:var(--dim)}
  .dot{width:8px;height:8px;border-radius:50%;background:#3d4a58}
  .dot.live{background:var(--green);box-shadow:0 0 0 3px rgba(63,185,80,.18)}
  .cards{display:grid;grid-template-columns:repeat(auto-fit,minmax(140px,1fr));
    gap:12px;margin-bottom:18px}
  .card{background:var(--panel);border:1px solid var(--line);border-radius:8px;padding:14px}
  .card .k{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.06em}
  .card .v{font-size:24px;font-weight:650;margin-top:4px;
    font-variant-numeric:tabular-nums}
  .card .d{color:var(--dim);font-size:12px;font-variant-numeric:tabular-nums}
  .panel{background:var(--panel);border:1px solid var(--line);border-radius:8px;
    padding:16px;margin-bottom:18px}
  .panel h2{font-size:14px;margin:0 0 2px}
  .panel .hint{color:var(--dim);font-size:12px;margin-bottom:12px}
  canvas{width:100%;height:240px;display:block}
  .legend{display:flex;gap:16px;font-size:12px;color:var(--dim);margin-top:8px;flex-wrap:wrap}
  .legend i{display:inline-block;width:10px;height:2px;vertical-align:middle;margin-right:6px}
  fieldset{border:1px solid var(--line);border-radius:8px;padding:14px;margin:0 0 18px}
  legend{color:var(--dim);font-size:11px;text-transform:uppercase;letter-spacing:.06em;padding:0 6px}
  .grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(160px,1fr));gap:12px}
  label{display:block;font-size:11px;color:var(--dim);margin-bottom:4px}
  input,select{width:100%;background:var(--bg);color:var(--fg);border:1px solid var(--line);
    border-radius:5px;padding:7px 9px;font-size:13px;font-family:inherit}
  input:disabled{opacity:.5}
  .msg{padding:10px 14px;border-radius:6px;font-size:13px;margin-bottom:14px;display:none}
  .msg.ok{background:rgba(63,185,80,.12);border:1px solid var(--green);color:#7ee288;display:block}
  .msg.err{background:rgba(255,68,56,.12);border:1px solid var(--red);color:#ff8b83;display:block}
  table{width:100%;border-collapse:collapse;font-size:12px;
    font-variant-numeric:tabular-nums}
  th,td{text-align:right;padding:5px 8px;border-bottom:1px solid var(--line)}
  th:first-child,td:first-child{text-align:left;
    font-family:ui-monospace,SFMono-Regular,Menlo,monospace}
  th{color:var(--dim);font-weight:550;position:sticky;top:0;background:var(--panel)}
  details{margin-bottom:18px}
  summary{cursor:pointer;color:var(--dim);font-size:13px;padding:6px 0}
  .scroll{max-height:300px;overflow:auto}
</style></head><body>

<header>
  <h1>Redis stream service</h1>
  <span class="sub" id="target"></span>
  <span style="flex:1"></span>
  <span class="pill"><span class="dot" id="pdot"></span><span id="pstat">producer stopped</span></span>
  <span class="pill"><span class="dot" id="cdot"></span><span id="cstat">consumer stopped</span></span>
</header>

<div class="wrap">
  <div class="msg" id="msg"></div>

  <div class="row">
    <button id="pbtn">Start producer</button>
    <button id="cbtn">Start consumer</button>
    <button id="allbtn" class="ghost">Stop all</button>
    <button id="pingbtn" class="ghost">Test connection</button>
    <button id="resetbtn" class="ghost">Reset metrics</button>
    <span style="flex:1"></span>
    <label style="margin:0;display:flex;align-items:center;gap:6px;color:var(--dim)">
      <input type="checkbox" id="autorefresh" checked style="width:auto">auto refresh
    </label>
    <select id="window" style="width:auto">
      <option value="60">1 min</option>
      <option value="120" selected>2 min</option>
      <option value="300">5 min</option>
      <option value="600">10 min</option>
    </select>
  </div>

  <div class="cards" id="cards"></div>

  <div class="panel">
    <h2>Throughput</h2>
    <div class="hint">Messages per second, one point per second. The current,
      still-filling second is excluded so there is no false dip at the right edge.</div>
    <canvas id="tp"></canvas>
    <div class="legend">
      <span><i style="background:var(--red)"></i>produced/s</span>
      <span><i style="background:var(--blue)"></i>consumed/s</span>
    </div>
  </div>

  <div class="panel">
    <h2>End-to-end latency</h2>
    <div class="hint">Milliseconds from XADD to XACK, from the
      <code>produced_at_ms</code> field on each entry. Gaps mean no traffic.</div>
    <canvas id="lt"></canvas>
    <div class="legend">
      <span><i style="background:var(--green)"></i>p50</span>
      <span><i style="background:var(--amber)"></i>p95</span>
      <span><i style="background:var(--red)"></i>max</span>
    </div>
  </div>

  <fieldset><legend>Connection and tuning</legend>
    <div class="grid" id="settings"></div>
    <div style="margin-top:12px;color:var(--dim);font-size:12px" id="lockhint"></div>
  </fieldset>

  <details><summary>Stream status from Redis (XLEN / XPENDING)</summary>
    <div class="panel" style="margin-top:10px">
      <button id="statusbtn" class="ghost">Read stream status</button>
      <div id="statusout" style="margin-top:12px;color:var(--dim);font-size:13px">
        Kept behind a button so the dashboard adds no Redis traffic on every refresh.
      </div>
    </div>
  </details>

  <details><summary>Per-second detail</summary>
    <div class="panel scroll" style="margin-top:10px"><table id="detail"></table></div>
  </details>
</div>

<script>
const $ = id => document.getElementById(id);
let SNAP = null, BUSY = false;

const FIELDS = [
  ["host","Host","text",1],["port","Port","number",1],["db","DB","number",1],
  ["username","Username","text",1],["password","Password","password",1],
  ["stream","Stream key","text",1],["group","Consumer group","text",1],
  ["consumer_name","Consumer name","text",1],
  ["rate","Rate (msg/s)","number",0],["ttl_seconds","TTL (seconds)","number",0],
  ["trim_interval","Trim interval (s)","number",0],
  ["batch_size","Batch size","number",0],["block_ms","Block (ms)","number",0],
];

function flash(text, ok){
  const m = $("msg");
  m.textContent = text;
  m.className = "msg " + (ok ? "ok" : "err");
  clearTimeout(flash._t);
  flash._t = setTimeout(() => { m.className = "msg"; }, 6000);
}

async function api(path, body){
  const opts = body ? {method:"POST", headers:{"Content-Type":"application/json"},
                       body: JSON.stringify(body)} : {};
  const r = await fetch(path, opts);
  if(!r.ok) throw new Error("HTTP " + r.status);
  return r.json();
}

function buildSettings(){
  const box = $("settings");
  if(box.dataset.built) return;
  box.dataset.built = "1";
  box.innerHTML = FIELDS.map(([k,label,type]) =>
    `<div><label for="f_${k}">${label}</label>
     <input id="f_${k}" name="${k}" type="${type}" step="any"></div>`).join("");
  // Push edits to the server on change; the server ignores changes while running.
  FIELDS.forEach(([k]) => $("f_"+k).addEventListener("change", async e => {
    let v = e.target.value;
    if(e.target.type === "number") v = v === "" ? null : Number(v);
    try{
      const r = await api("/api/settings", {[k]: v});
      if(r.ignored && r.ignored.length) flash("Stop both workers to change " + r.ignored.join(", "), false);
    }catch(err){ flash(String(err), false); }
  }));
}

function fillSettings(s, lock){
  FIELDS.forEach(([k,,,lockable]) => {
    const el = $("f_"+k);
    if(document.activeElement === el) return;      // do not fight the user's cursor
    el.value = (s[k] === null || s[k] === undefined) ? "" : s[k];
    el.disabled = !!(lock && lockable);
  });
  $("lockhint").textContent = lock
    ? "Connection and stream fields are locked while a worker is running."
    : "";
}

function card(k, v, d){
  return `<div class="card"><div class="k">${k}</div><div class="v">${v}</div>` +
         (d ? `<div class="d">${d}</div>` : "") + `</div>`;
}
const n = x => (x === null || x === undefined) ? "-" : x.toLocaleString();
const ms = x => (x === null || x === undefined) ? "-" : Math.round(x) + " ms";

function renderCards(s){
  const t = s.totals, r = s.rates, l = s.latency_ms;
  $("cards").innerHTML =
    card("Produced", n(t.produced), r.produce_per_s.toFixed(1) + "/s") +
    card("Consumed", n(t.consumed), r.consume_per_s.toFixed(1) + "/s") +
    card("Acked", n(t.acked), r.ack_per_s.toFixed(1) + "/s") +
    card("Lag p50", ms(l.p50), l.samples ? l.samples.toLocaleString() + " samples" : "") +
    card("Lag p95", ms(l.p95), "") +
    card("Lag max", ms(l.max), "") +
    card("In flight", n(t.produced - t.consumed), "produced - consumed") +
    card("Trimmed by TTL", n(t.trimmed), "") +
    card("Failed", n(t.failed), "") +
    card("Errors", n(t.errors), "");
}

// --- Hand rolled canvas line chart ---------------------------------------
// A charting library would mean a pip install or a CDN fetch, and this bundle
// can have neither. This is deliberately minimal: axes, gridlines, lines,
// gaps for null values.

function chart(canvas, rows, lines){
  const dpr = window.devicePixelRatio || 1;
  const cssW = canvas.clientWidth, cssH = canvas.clientHeight;
  canvas.width = cssW * dpr; canvas.height = cssH * dpr;
  const g = canvas.getContext("2d");
  g.setTransform(dpr,0,0,dpr,0,0);
  g.clearRect(0,0,cssW,cssH);

  const pad = {l:52, r:12, t:10, b:24};
  const W = cssW - pad.l - pad.r, H = cssH - pad.t - pad.b;
  const css = getComputedStyle(document.documentElement);
  const dim = css.getPropertyValue("--dim").trim();
  const lineCol = css.getPropertyValue("--line").trim();

  if(!rows.length){ return; }

  let max = 0;
  for(const row of rows) for(const ln of lines){
    const v = row[ln.key];
    if(v !== null && v !== undefined && v > max) max = v;
  }
  if(max <= 0) max = 1;
  // Round the axis up to something readable rather than a raw maximum.
  const mag = Math.pow(10, Math.floor(Math.log10(max)));
  max = Math.ceil(max / mag) * mag;

  g.strokeStyle = lineCol; g.fillStyle = dim;
  g.font = "11px ui-monospace,Menlo,monospace"; g.lineWidth = 1;
  for(let i = 0; i <= 4; i++){
    const y = pad.t + H - (H * i / 4);
    g.beginPath(); g.moveTo(pad.l, y); g.lineTo(pad.l + W, y); g.stroke();
    g.textAlign = "right"; g.textBaseline = "middle";
    g.fillText(String(Math.round(max * i / 4)), pad.l - 8, y);
  }

  const x = i => pad.l + (rows.length === 1 ? W/2 : W * i / (rows.length - 1));
  const y = v => pad.t + H - H * (v / max);

  // Time labels at both ends and the middle.
  g.textBaseline = "top"; g.fillStyle = dim;
  const label = i => new Date(rows[i].t * 1000).toLocaleTimeString();
  g.textAlign = "left";   g.fillText(label(0), pad.l, pad.t + H + 6);
  if(rows.length > 2){
    g.textAlign = "center";
    g.fillText(label(Math.floor(rows.length/2)), pad.l + W/2, pad.t + H + 6);
  }
  g.textAlign = "right";  g.fillText(label(rows.length-1), pad.l + W, pad.t + H + 6);

  for(const ln of lines){
    g.strokeStyle = css.getPropertyValue(ln.varName).trim();
    g.lineWidth = 2; g.lineJoin = "round";
    g.beginPath();
    let drawing = false;
    rows.forEach((row, i) => {
      const v = row[ln.key];
      if(v === null || v === undefined){ drawing = false; return; }  // leave a gap
      if(!drawing){ g.moveTo(x(i), y(v)); drawing = true; }
      else g.lineTo(x(i), y(v));
    });
    g.stroke();
  }
}

function renderDetail(rows){
  const cols = ["produced","consumed","acked","failed","trimmed","lag_p50","lag_p95","lag_max"];
  const head = "<tr><th>time</th>" + cols.map(c=>`<th>${c}</th>`).join("") + "</tr>";
  const body = rows.slice().reverse().slice(0,120).map(r =>
    "<tr><td>" + new Date(r.t*1000).toLocaleTimeString() + "</td>" +
    cols.map(c => `<td>${r[c] === null || r[c] === undefined ? "" : r[c]}</td>`).join("") +
    "</tr>").join("");
  $("detail").innerHTML = head + body;
}

function renderStatus(s){
  const pr = s.producer_running, cr = s.consumer_running;
  $("pdot").className = "dot" + (pr ? " live" : "");
  $("cdot").className = "dot" + (cr ? " live" : "");
  $("pstat").textContent = "producer " + (pr ? "running" : "stopped");
  $("cstat").textContent = "consumer " + (cr ? "running" : "stopped");
  $("pbtn").textContent = pr ? "Stop producer" : "Start producer";
  $("cbtn").textContent = cr ? "Stop consumer" : "Start consumer";
  $("pbtn").className = pr ? "on" : "";
  $("cbtn").className = cr ? "on" : "";
  $("allbtn").disabled = !(pr || cr);
  const st = s.settings;
  $("target").textContent = st.stream + " @ " + st.host + ":" + st.port +
    "  ·  group " + st.group + "  ·  TTL " + st.ttl_seconds + "s";
  fillSettings(st, pr || cr);
}

async function refresh(){
  if(BUSY) return;
  try{
    const w = $("window").value;
    const s = await api("/api/metrics?window=" + w);
    SNAP = s;
    renderStatus(s.status);
    renderCards(s);
    chart($("tp"), s.series, [
      {key:"produced", varName:"--red"}, {key:"consumed", varName:"--blue"}]);
    chart($("lt"), s.series, [
      {key:"lag_p50", varName:"--green"}, {key:"lag_p95", varName:"--amber"},
      {key:"lag_max", varName:"--red"}]);
    renderDetail(s.series);
    if(s.last_error && !refresh._shown){ flash("Last error: " + s.last_error, false); refresh._shown = s.last_error; }
  }catch(err){ /* transient during restart; the next tick retries */ }
}

async function act(path){
  BUSY = true;
  try{
    const r = await api(path, {});
    if(r.message) flash(r.message, r.ok !== false);
  }catch(err){ flash(String(err), false); }
  finally{ BUSY = false; refresh(); }
}

$("pbtn").onclick = () => act(SNAP && SNAP.status.producer_running
  ? "/api/producer/stop" : "/api/producer/start");
$("cbtn").onclick = () => act(SNAP && SNAP.status.consumer_running
  ? "/api/consumer/stop" : "/api/consumer/start");
$("allbtn").onclick = () => act("/api/stop_all");
$("pingbtn").onclick = () => act("/api/ping");
$("resetbtn").onclick = () => act("/api/reset");
$("statusbtn").onclick = async () => {
  $("statusout").textContent = "reading...";
  try{
    const s = await api("/api/stream_status");
    $("statusout").innerHTML = s.error ? `<span style="color:var(--red)">${s.error}</span>`
      : `Length <b>${n(s.length)}</b> &nbsp; Pending <b>${s.pending === null ? "group not created" : n(s.pending)}</b>` +
        (s.consumers && s.consumers.length
          ? "<br>Consumers: " + s.consumers.map(c=>`${c.name} (${c.pending})`).join(", ") : "");
  }catch(err){ $("statusout").textContent = String(err); }
};
$("window").onchange = refresh;
window.addEventListener("resize", () => { if(SNAP) refresh(); });

buildSettings();
refresh();
setInterval(() => { if($("autorefresh").checked) refresh(); }, 2000);
</script></body></html>
"""


# --- HTTP handler ----------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "RedisStreamDashboard/1.0"
    protocol_version = "HTTP/1.1"

    # --- helpers ---
    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass  # browser navigated away mid-response

    def _json(self, payload: dict, code: int = 200) -> None:
        self._send(code, json.dumps(payload).encode("utf-8"), "application/json")

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if not length:
            return {}
        try:
            return json.loads(self.rfile.read(length) or b"{}")
        except json.JSONDecodeError:
            return {}

    def log_message(self, fmt: str, *args: object) -> None:
        log.debug("%s - %s", self.address_string(), fmt % args)

    # --- routes ---
    def do_GET(self) -> None:  # noqa: N802
        route = urlparse(self.path)
        if route.path in ("/", "/index.html"):
            self._send(200, PAGE.encode("utf-8"), "text/html; charset=utf-8")
            return
        if route.path == "/api/metrics":
            qs = parse_qs(route.query)
            try:
                window = max(10, min(600, int(qs.get("window", ["120"])[0])))
            except ValueError:
                window = 120
            snap = MANAGER.metrics.snapshot(window_seconds=window)
            snap["status"] = MANAGER.status()
            self._json(snap)
            return
        if route.path == "/api/stream_status":
            self._json(MANAGER.stream_status())
            return
        if route.path == "/api/health":
            self._json({"ok": True})
            return
        self._json({"error": "not found"}, 404)

    def do_POST(self) -> None:  # noqa: N802
        path = urlparse(self.path).path
        try:
            if path == "/api/producer/start":
                ok, message = MANAGER.ping()
                if not ok:
                    self._json({"ok": False, "message": f"not starting: {message}"})
                    return
                started = MANAGER.start_producer()
                self._json({"ok": True,
                            "message": "producer started" if started
                                       else "producer already running"})
                return
            if path == "/api/producer/stop":
                MANAGER.stop_producer()
                self._json({"ok": True, "message": "producer stopped"})
                return
            if path == "/api/consumer/start":
                ok, message = MANAGER.ping()
                if not ok:
                    self._json({"ok": False, "message": f"not starting: {message}"})
                    return
                started = MANAGER.start_consumer()
                self._json({"ok": True,
                            "message": "consumer started" if started
                                       else "consumer already running"})
                return
            if path == "/api/consumer/stop":
                MANAGER.stop_consumer()
                self._json({"ok": True, "message": "consumer stopped"})
                return
            if path == "/api/stop_all":
                MANAGER.stop_all()
                self._json({"ok": True, "message": "both workers stopped"})
                return
            if path == "/api/reset":
                MANAGER.metrics.reset()
                self._json({"ok": True, "message": "metrics reset"})
                return
            if path == "/api/ping":
                ok, message = MANAGER.ping()
                self._json({"ok": ok, "message": message})
                return
            if path == "/api/stream_status":
                self._json(MANAGER.stream_status())
                return
            if path == "/api/settings":
                self._json(self._update_settings(self._read_json()))
                return
        except Exception as exc:  # noqa: BLE001
            log.exception("request failed")
            self._json({"ok": False, "message": f"{type(exc).__name__}: {exc}"}, 500)
            return
        self._json({"error": "not found"}, 404)

    # --- settings ---
    LOCKED_WHILE_RUNNING = {"host", "port", "db", "username", "password",
                            "stream", "group", "consumer_name", "ssl", "ssl_verify"}
    NUMERIC = {"port": int, "db": int, "ttl_seconds": int, "max_len": int,
               "trim_interval": float, "rate": float, "batch_size": int,
               "block_ms": int, "claim_min_idle_ms": int, "claim_interval": float}

    def _update_settings(self, body: dict) -> dict:
        running = MANAGER.producer_running() or MANAGER.consumer_running()
        applied, ignored = [], []
        for key, value in body.items():
            if key not in MANAGER.settings:
                continue
            if running and key in self.LOCKED_WHILE_RUNNING:
                # Changing the endpoint under a live worker would silently apply
                # to the next reconnect only, which is worse than refusing.
                ignored.append(key)
                continue
            if key in self.NUMERIC and value is not None:
                try:
                    value = self.NUMERIC[key](value)
                except (TypeError, ValueError):
                    ignored.append(key)
                    continue
            if isinstance(value, str) and value == "" and key in (
                    "username", "password"):
                value = None
            MANAGER.settings[key] = value
            applied.append(key)
        return {"ok": True, "applied": applied, "ignored": ignored}


# --- Entry point -----------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Zero-dependency dashboard for the Redis stream service.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--host", default=config.REDIS_HOST, help="Redis host")
    p.add_argument("--port", type=int, default=config.REDIS_PORT, help="Redis port")
    p.add_argument("--db", type=int, default=config.REDIS_DB)
    p.add_argument("--username", default=config.REDIS_USERNAME)
    p.add_argument("--password", default=config.REDIS_PASSWORD)
    p.add_argument("--tls", action="store_true", default=config.REDIS_SSL)
    p.add_argument("--tls-no-verify", action="store_true",
                   default=not config.REDIS_SSL_VERIFY)
    p.add_argument("--stream", default=config.STREAM_KEY)
    p.add_argument("--group", default=config.CONSUMER_GROUP)
    p.add_argument("--rate", type=float, default=config.PRODUCE_RATE)
    p.add_argument("--ttl", type=int, default=config.TTL_SECONDS)
    p.add_argument("--bind", default="127.0.0.1",
                   help="dashboard listen address; 0.0.0.0 to expose it")
    p.add_argument("--web-port", type=int, default=8080,
                   help="dashboard HTTP port")
    p.add_argument("--open", action="store_true", help="open a browser on start")
    p.add_argument("--log-level", default=config.LOG_LEVEL)
    return p.parse_args(argv)


def build_server(args: argparse.Namespace) -> ThreadingHTTPServer:
    MANAGER.settings.update(
        host=args.host, port=args.port, db=args.db,
        username=args.username or None, password=args.password or None,
        ssl=args.tls, ssl_verify=not args.tls_no_verify,
        stream=args.stream, group=args.group,
        rate=args.rate, ttl_seconds=args.ttl,
    )
    ThreadingHTTPServer.allow_reuse_address = True
    ThreadingHTTPServer.daemon_threads = True
    return ThreadingHTTPServer((args.bind, args.web_port), Handler)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s")

    try:
        httpd = build_server(args)
    except OSError as exc:
        log.error("cannot bind %s:%s -- %s", args.bind, args.web_port, exc)
        return 1

    url = f"http://{'127.0.0.1' if args.bind in ('0.0.0.0', '') else args.bind}:{args.web_port}"
    print(f"\n  Dashboard:  {url}")
    print(f"  Redis:      {args.host}:{args.port}  stream {args.stream!r}")
    print("  Ctrl-C to stop\n")

    if args.open:
        threading.Thread(target=lambda: webbrowser.open(url), daemon=True).start()

    try:
        httpd.serve_forever()
    except KeyboardInterrupt:
        print("\nstopping workers...")
    finally:
        MANAGER.stop_all()
        httpd.shutdown()
        httpd.server_close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
