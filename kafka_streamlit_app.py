#!/usr/bin/env python3
"""Streamlit control panel and performance dashboard for the Kafka topics.

    pip install streamlit kafka-python
    streamlit run kafka_streamlit_app.py

Talks to a real Kafka-protocol broker -- Kafka itself, or a compatible one
like Korvet -- over `--bootstrap-servers` / the sidebar field (default
`localhost:9092`). If your broker is Korvet, this is the Streamlit
equivalent of:

    korvet topics --list --bootstrap-server localhost:9092

("List broker topics" in the sidebar runs exactly that query.) If Streamlit
or kafka-python cannot be installed on this host, `kafka_dashboard.py` gives
the same buttons, charts and topics table with only the kafka-python
dependency and no Streamlit.

Start/stop toggles run the producer and consumer as background threads inside
this process (see kafka_workers.py), so the charts read from the same
in-memory Metrics + per-topic counters the workers write to. Nothing is
polled out of Kafka just to draw the charts -- only the on-demand "List
broker topics" button talks to the broker outside of the workers themselves.
"""

from __future__ import annotations

import sys
import time

try:
    import streamlit as st
except ModuleNotFoundError:  # pragma: no cover - guidance, not logic
    sys.stderr.write(
        "\nStreamlit is not installed.\n\n"
        "  pip install streamlit\n"
        "  No Streamlit at all:  python kafka_dashboard.py   "
        "(zero-dependency alternative, still needs kafka-python)\n\n")
    raise SystemExit(1)

from kafka_topics import all_topics       # noqa: E402
from kafka_workers import KafkaWorkerManager  # noqa: E402
from metrics import Metrics                # noqa: E402

REFRESH_SECONDS = 2

st.set_page_config(page_title="Kafka topics service", page_icon="•",
                   layout="wide")


# --- One manager for the whole session ------------------------------------

@st.cache_resource
def get_manager() -> KafkaWorkerManager:
    """Streamlit reruns this script top to bottom on every interaction.

    cache_resource keeps a single KafkaWorkerManager (and therefore one set of
    worker threads and one Metrics instance) alive across those reruns.
    Without it, every button click would start a fresh manager and orphan the
    threads.
    """
    return KafkaWorkerManager(Metrics(window_seconds=600))


mgr = get_manager()


# --- Sidebar: connection and tuning ---------------------------------------

with st.sidebar:
    st.header("Connection")
    busy = mgr.producer_running() or mgr.consumer_running()
    if busy:
        st.caption("Stop both workers to change connection settings.")

    s = mgr.settings
    s["bootstrap_servers"] = st.text_input(
        "Bootstrap servers", s["bootstrap_servers"], disabled=busy,
        help="host:port of Kafka or a Kafka-compatible broker (e.g. Korvet).")
    s["group"] = st.text_input("Consumer group", s["group"], disabled=busy)
    s["client_id"] = st.text_input("Producer client id", s["client_id"], disabled=busy)
    s["consumer_client_id"] = st.text_input(
        "Consumer client id", s["consumer_client_id"], disabled=busy)
    s["api_version"] = st.text_input(
        "API version (blank = auto-detect)", s.get("api_version", ""), disabled=busy,
        help="Try e.g. 0.10.1 if 'List broker topics' works but starting the "
             "producer/consumer times out with KafkaTimeoutError -- that "
             "usually means kafka-python's protocol auto-negotiation picked a "
             "version the broker does not fully support.")

    if st.button("Test connection", use_container_width=True):
        ok, message = mgr.ping()
        (st.success if ok else st.error)(message)

    if st.button("List broker topics", use_container_width=True,
                 help="Equivalent to: korvet topics --list "
                      "--bootstrap-server " + str(s["bootstrap_servers"])):
        ok, topics_or_error = mgr.list_broker_topics()
        if ok:
            st.success(f"{len(topics_or_error)} topic(s) on the broker")
            st.code("\n".join(topics_or_error) or "(none yet)")
        else:
            st.error(topics_or_error)

    if st.button("Create this app's topics", use_container_width=True,
                 help="Runs the equivalent of korvet topics --create for each "
                      "topic this app uses. The producer also does this "
                      "automatically before its first send(); use this if you "
                      "want it done up front, or if auto-create failed."):
        ok, message = mgr.ensure_topics()
        (st.success if ok else st.error)(message)

    st.divider()
    st.header("Producer")
    s["rate"] = st.slider("Rate (msg/s)", 0.5, 200.0, float(s["rate"]), 0.5,
                          disabled=mgr.producer_running(),
                          help="Applied when the producer starts.")
    s["acks"] = st.selectbox("Producer acks", ["all", "1", "0"],
                             index=["all", "1", "0"].index(str(s["acks"]))
                             if str(s["acks"]) in ("all", "1", "0") else 0,
                             disabled=mgr.producer_running())

    st.divider()
    st.header("Consumer")
    s["auto_offset_reset"] = st.selectbox(
        "Auto offset reset", ["earliest", "latest"],
        index=["earliest", "latest"].index(s["auto_offset_reset"])
        if s["auto_offset_reset"] in ("earliest", "latest") else 0,
        disabled=mgr.consumer_running())
    s["max_records"] = st.number_input("Max records / poll", 1, 5000,
                                       int(s["max_records"]),
                                       disabled=mgr.consumer_running())
    s["poll_ms"] = st.number_input("Poll (ms)", 100, 60000, int(s["poll_ms"]),
                                   disabled=mgr.consumer_running())
    s["max_block_ms"] = st.number_input(
        "Producer max block (ms)", 1000, 120000, int(s.get("max_block_ms", 10000)),
        disabled=mgr.producer_running(),
        help="How long send() blocks on metadata before raising "
             "KafkaTimeoutError. Lower this to fail fast while debugging.")

    st.divider()
    auto = st.checkbox("Auto refresh", True)
    window = st.select_slider("Chart window",
                              options=[60, 120, 300, 600], value=120,
                              format_func=lambda v: f"{v // 60} min" if v >= 60 else f"{v}s")
    if st.button("Reset metrics", use_container_width=True):
        mgr.metrics.reset()
        mgr.topic_counters.reset()
        st.toast("Metrics reset")


# --- Header and controls ---------------------------------------------------

st.title("Kafka topics service")
st.caption(f"`{s['bootstrap_servers']}`  ·  group `{s['group']}`  ·  "
           f"{len(all_topics())} topics routed by DocRequestCode")

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
m3.metric("Committed", f"{totals['acked']:,}", f"{rates['ack_per_s']:.1f}/s")
m4.metric("Lag p50", f"{lat['p50']:.0f} ms" if lat["p50"] is not None else "-")
m5.metric("Lag p95", f"{lat['p95']:.0f} ms" if lat["p95"] is not None else "-")
m6.metric("Lag max", f"{lat['max']:.0f} ms" if lat["max"] is not None else "-")

backlog = totals["produced"] - totals["consumed"]
n1, n2, n3, n4 = st.columns(4)
n1.metric("In flight", f"{backlog:,}",
          help="Produced minus consumed since the metrics were last reset.")
n2.metric("Topics", f"{len(all_topics())}")
n3.metric("Failed handles", f"{totals['failed']:,}")
n4.metric("Errors", f"{totals['errors']:,}")


# --- Per-topic breakdown ----------------------------------------------------

st.subheader("Topics")
st.caption("Each generated application document is routed to the topic matching "
          "its own `DocRequestCode`, so this is the \"put them into topics\" view: "
          "one producer, one consumer, several topics.")

topic_rows = mgr.topics_status()
table = [
    {
        "topic": topic,
        "produced": row["produced"],
        "consumed": row["consumed"],
        "in flight": row["produced"] - row["consumed"],
        "failed": row["failed"],
        "last partition": row["last_partition"],
        "last offset": row["last_offset"],
        "last seen": (time.strftime("%H:%M:%S", time.localtime(row["last_seen"]))
                     if row["last_seen"] else "-"),
    }
    for topic, row in topic_rows.items()
]
st.dataframe(table, use_container_width=True, hide_index=True)


# --- Charts ---------------------------------------------------------------

series = snap["series"]

if not series or all(r["produced"] == 0 and r["consumed"] == 0 for r in series):
    st.info("No traffic yet. Start the producer and consumer to populate the charts.")
else:
    import datetime as _dt

    labels = [_dt.datetime.fromtimestamp(r["t"]).strftime("%H:%M:%S") for r in series]

    st.subheader("Throughput")
    st.caption("Messages per second across all topics combined, one point per "
              "second. The current, still-filling second is excluded.")
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
    st.caption("Milliseconds from the producer's send() to the consumer "
              "committing the offset, derived from the `produced_at_ms` field "
              "on each record. Gaps mean no traffic.")
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


# --- Auto refresh ----------------------------------------------------------

if auto and (mgr.producer_running() or mgr.consumer_running()):
    time.sleep(REFRESH_SECONDS)
    st.rerun()
elif auto:
    st.caption("Auto refresh pauses while both workers are stopped.")
