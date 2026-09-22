#!/usr/bin/env python3
"""Zero-dependency control panel and performance dashboard for the Kafka topics.

Same look, same buttons, same charts as dashboard.py (the Redis Streams
version), plus a per-topic breakdown table since this backend fans a single
producer/consumer pair out across several Kafka topics instead of one stream.

    python kafka_dashboard.py --bootstrap-servers localhost:9092
    # then open http://127.0.0.1:8081

Needs a real Kafka broker reachable at --bootstrap-servers; there is no
in-process fallback. kafka-python must be installed (`pip install kafka-python`).
"""

from __future__ import annotations

import argparse
import json
import logging
import sys
import threading
import webbrowser
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from urllib.parse import parse_qs, urlparse

import config
from kafka_topics import all_topics
from kafka_workers import KafkaWorkerManager
from metrics import Metrics

log = logging.getLogger("kafka_dashboard")

MANAGER: KafkaWorkerManager = KafkaWorkerManager(Metrics(window_seconds=600))


# --- Front end ---------------------------------------------------------

PAGE = r"""<!DOCTYPE html>
<html lang="en"><head><meta charset="utf-8">
<title>Kafka topics service</title>
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
  <h1>Kafka topics service</h1>
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
    <h2>Topics</h2>
    <div class="hint">Each generated application document is routed to the topic
      matching its own <code>DocRequestCode</code>, so this table is the "put them
      into topics" view: one producer, one consumer, several topics.</div>
    <table id="topics"></table>
  </div>

  <div class="panel">
    <h2>Throughput</h2>
    <div class="hint">Messages per second across all topics combined, one point
      per second. The current, still-filling second is excluded so there is no
      false dip at the right edge.</div>
    <canvas id="tp"></canvas>
    <div class="legend">
      <span><i style="background:var(--red)"></i>produced/s</span>
      <span><i style="background:var(--blue)"></i>consumed/s</span>
    </div>
  </div>

  <div class="panel">
    <h2>End-to-end latency</h2>
    <div class="hint">Milliseconds from the producer's send() to the consumer
      committing the offset, from the <code>produced_at_ms</code> field on each
      record. Gaps mean no traffic.</div>
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

  <details><summary>Broker topics</summary>
    <div class="panel" style="margin-top:10px">
      <button id="topicsbtn" class="ghost">List broker topics</button>
      <button id="createtopicsbtn" class="ghost">Create this app's topics</button>
      <div id="topicsout" style="margin-top:12px;color:var(--dim);font-size:13px">
        "List" queries the broker's own metadata (equivalent to
        <code>korvet topics --list --bootstrap-server ...</code>), not just the
        topics this app publishes to. "Create" runs the equivalent of
        <code>korvet topics --create</code> for each of this app's topics --
        useful if your broker does not auto-create a topic on first send(),
        which surfaces as producer send() timing out with
        <code>KafkaTimeoutError</code>. The producer also does this
        automatically on its first send. Kept behind buttons so refresh adds
        no broker traffic.
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
  ["bootstrap_servers","Bootstrap servers","text",1],
  ["group","Consumer group","text",1],
  ["client_id","Producer client id","text",1],
  ["consumer_client_id","Consumer client id","text",1],
  ["rate","Rate (msg/s)","number",0],
  ["acks","Producer acks","text",1],
  ["auto_offset_reset","Auto offset reset","text",1],
  ["max_records","Max records/poll","number",0],
  ["poll_ms","Poll (ms)","number",0],
  ["max_block_ms","Producer max block (ms)","number",0],
  ["api_version","API version (blank=auto)","text",1],
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
    if(document.activeElement === el) return;
    el.value = (s[k] === null || s[k] === undefined) ? "" : s[k];
    el.disabled = !!(lock && lockable);
  });
  $("lockhint").textContent = lock
    ? "Connection and group fields are locked while a worker is running."
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
    card("Committed", n(t.acked), r.ack_per_s.toFixed(1) + "/s") +
    card("Lag p50", ms(l.p50), l.samples ? l.samples.toLocaleString() + " samples" : "") +
    card("Lag p95", ms(l.p95), "") +
    card("Lag max", ms(l.max), "") +
    card("In flight", n(t.produced - t.consumed), "produced - consumed") +
    card("Topics", s.topics ? s.topics.length : 0, "") +
    card("Failed", n(t.failed), "") +
    card("Errors", n(t.errors), "");
}

function renderTopics(topics){
  const rows = Object.entries(topics || {});
  const head = "<tr><th>topic</th><th>produced</th><th>consumed</th>" +
    "<th>in flight</th><th>failed</th><th>last partition</th>" +
    "<th>last offset</th><th>last seen</th></tr>";
  const body = rows.map(([topic, r]) =>
    `<tr><td>${topic}</td><td>${n(r.produced)}</td><td>${n(r.consumed)}</td>` +
    `<td>${n(r.produced - r.consumed)}</td><td>${n(r.failed)}</td>` +
    `<td>${r.last_partition === null ? "-" : r.last_partition}</td>` +
    `<td>${r.last_offset === null ? "-" : r.last_offset}</td>` +
    `<td>${r.last_seen ? new Date(r.last_seen*1000).toLocaleTimeString() : "-"}</td></tr>`
  ).join("");
  $("topics").innerHTML = head + (body || "<tr><td colspan=8>no topics yet</td></tr>");
}

// --- Hand rolled canvas line chart, same as the Redis Streams dashboard ---

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
      if(v === null || v === undefined){ drawing = false; return; }
      if(!drawing){ g.moveTo(x(i), y(v)); drawing = true; }
      else g.lineTo(x(i), y(v));
    });
    g.stroke();
  }
}

function renderDetail(rows){
  const cols = ["produced","consumed","acked","failed","lag_p50","lag_p95","lag_max"];
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
  $("target").textContent = st.bootstrap_servers + "  ·  group " + st.group +
    "  ·  " + (s.topics ? s.topics.length : 0) + " topics";
  fillSettings(st, pr || cr);
}

async function refresh(){
  if(BUSY) return;
  try{
    const w = $("window").value;
    const s = await api("/api/metrics?window=" + w);
    SNAP = s;
    renderStatus(s.status);
    renderCards(Object.assign({}, s, {topics: s.status.topics}));
    renderTopics(s.topic_counters);
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
$("topicsbtn").onclick = async () => {
  $("topicsout").textContent = "listing...";
  try{
    const r = await api("/api/broker_topics");
    $("topicsout").innerHTML = r.ok
      ? (r.topics.length ? "Broker topics: " + r.topics.map(t=>`<code>${t}</code>`).join(", ")
                          : "Broker has no topics yet.")
      : `<span style="color:var(--red)">${r.message}</span>`;
  }catch(err){ $("topicsout").textContent = String(err); }
};
$("createtopicsbtn").onclick = async () => {
  $("topicsout").textContent = "creating...";
  try{
    const r = await api("/api/create_topics", {});
    $("topicsout").innerHTML = r.ok
      ? `<span style="color:var(--green)">${r.message}</span>`
      : `<span style="color:var(--red)">${r.message}</span>`;
  }catch(err){ $("topicsout").textContent = String(err); }
};
$("window").onchange = refresh;
window.addEventListener("resize", () => { if(SNAP) refresh(); });

buildSettings();
refresh();
setInterval(() => { if($("autorefresh").checked) refresh(); }, 2000);
</script></body></html>
"""


# --- HTTP handler --------------------------------------------------------

class Handler(BaseHTTPRequestHandler):
    server_version = "KafkaTopicsDashboard/1.0"
    protocol_version = "HTTP/1.1"

    def _send(self, code: int, body: bytes, ctype: str) -> None:
        self.send_response(code)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(body)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        try:
            self.wfile.write(body)
        except (BrokenPipeError, ConnectionResetError):
            pass

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
            snap["topic_counters"] = MANAGER.topics_status()
            self._json(snap)
            return
        if route.path == "/api/topics":
            self._json({"topics": all_topics(), "counters": MANAGER.topics_status()})
            return
        if route.path == "/api/broker_topics":
            ok, topics_or_error = MANAGER.list_broker_topics()
            if ok:
                self._json({"ok": True, "topics": topics_or_error})
            else:
                self._json({"ok": False, "message": topics_or_error})
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
                MANAGER.topic_counters.reset()
                self._json({"ok": True, "message": "metrics reset"})
                return
            if path == "/api/ping":
                ok, message = MANAGER.ping()
                self._json({"ok": ok, "message": message})
                return
            if path == "/api/create_topics":
                ok, message = MANAGER.ensure_topics()
                self._json({"ok": ok, "message": message})
                return
            if path == "/api/settings":
                self._json(self._update_settings(self._read_json()))
                return
        except Exception as exc:  # noqa: BLE001
            log.exception("request failed")
            self._json({"ok": False, "message": f"{type(exc).__name__}: {exc}"}, 500)
            return
        self._json({"error": "not found"}, 404)

    LOCKED_WHILE_RUNNING = {"bootstrap_servers", "group", "client_id",
                            "consumer_client_id", "api_version"}
    NUMERIC = {"rate": float, "max_records": int, "poll_ms": int, "max_block_ms": int}

    def _update_settings(self, body: dict) -> dict:
        running = MANAGER.producer_running() or MANAGER.consumer_running()
        applied, ignored = [], []
        for key, value in body.items():
            if key not in MANAGER.settings:
                continue
            if running and key in self.LOCKED_WHILE_RUNNING:
                ignored.append(key)
                continue
            if key in self.NUMERIC and value is not None:
                try:
                    value = self.NUMERIC[key](value)
                except (TypeError, ValueError):
                    ignored.append(key)
                    continue
            MANAGER.settings[key] = value
            applied.append(key)
        return {"ok": True, "applied": applied, "ignored": ignored}


# --- Entry point -----------------------------------------------------------

def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Zero-dependency dashboard for the Kafka topics simulation.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter)
    p.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS)
    p.add_argument("--group", default=config.KAFKA_CONSUMER_GROUP)
    p.add_argument("--rate", type=float, default=config.KAFKA_PRODUCE_RATE)
    p.add_argument("--bind", default="127.0.0.1",
                   help="dashboard listen address; 0.0.0.0 to expose it")
    p.add_argument("--web-port", type=int, default=8081,
                   help="dashboard HTTP port (default 8081, distinct from "
                        "dashboard.py's 8080 so both can run side by side)")
    p.add_argument("--open", action="store_true", help="open a browser on start")
    p.add_argument("--log-level", default=config.LOG_LEVEL)
    return p.parse_args(argv)


def build_server(args: argparse.Namespace) -> ThreadingHTTPServer:
    MANAGER.settings.update(
        bootstrap_servers=args.bootstrap_servers, group=args.group, rate=args.rate,
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
    print(f"  Kafka:      {args.bootstrap_servers}  topics {all_topics()}")
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
