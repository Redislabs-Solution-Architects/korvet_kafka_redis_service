#!/usr/bin/env python3
"""Tests for the Kafka topics producer/consumer/worker simulation.

Runs entirely against fake_kafka.py's in-memory broker -- no real Kafka
broker, no kafka-python install, no network. Exercises topic routing, the
producer envelope shape, a full produce/consume round trip with manual
offset commits, malformed-record handling, and KafkaWorkerManager's
start/stop lifecycle with its per-topic counters.

    python tests/test_kafka_service.py
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import config  # noqa: E402
import fake_kafka  # noqa: E402
import kafka_topics  # noqa: E402
from kafka_consumer import Consumer  # noqa: E402
from kafka_producer import Producer  # noqa: E402
from kafka_workers import KafkaWorkerManager  # noqa: E402
from metrics import Metrics  # noqa: E402
from payloads import generate_payload  # noqa: E402

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


# --- topic routing -----------------------------------------------------

def test_topic_routing() -> None:
    print("\n[topic routing]")
    expected = {
        "East-Bank": "bigcreditbank.applications.east-bank",
        "West-Bank": "bigcreditbank.applications.west-bank",
        "North-Bank": "bigcreditbank.applications.north-bank",
        "South-Bank": "bigcreditbank.applications.south-bank",
    }
    for code, topic in expected.items():
        check(f"{code} routes to {topic}",
              kafka_topics.topic_for_code(code) == topic)

    check("unknown code falls back to default topic",
          kafka_topics.topic_for_code("SOMETHING-NEW") == kafka_topics.DEFAULT_TOPIC)

    topics = kafka_topics.all_topics()
    check("all_topics has one entry per known code plus the default",
          len(topics) == len(expected) + 1, str(topics))
    check("all_topics has no duplicates", len(topics) == len(set(topics)), str(topics))

    payload = generate_payload(1)
    check("topic_for_payload agrees with topic_for_code",
          kafka_topics.topic_for_payload(payload)
          == kafka_topics.topic_for_code(payload["DocRequestCode"]))


def test_ensure_topics_exist_without_kafka_python() -> None:
    print("\n[ensure_topics_exist without kafka-python installed]")
    # This sandbox genuinely does not have kafka-python installed, so this
    # exercises the real ImportError branch, not a mock of it.
    try:
        import kafka  # noqa: F401
        print("  SKIP  kafka-python is installed in this environment, "
             "the ImportError branch cannot be exercised here")
        return
    except ImportError:
        pass
    ok, message = kafka_topics.ensure_topics_exist("dummy:9092")
    check("reports kafka-python missing rather than raising", ok is False)
    check("message says what is missing", "kafka-python" in message, message)


# --- config plumbing for api_version / max_block_ms -----------------------

def test_api_version_and_max_block_ms_config() -> None:
    print("\n[api_version / max_block_ms config]")
    check("blank string means auto-detect", config.parse_api_version("") is None)
    check("None means auto-detect", config.parse_api_version(None) is None)
    check("dotted version string parses to a tuple",
          config.parse_api_version("0.10.1") == (0, 10, 1))
    check("garbage falls back to auto-detect rather than raising",
          config.parse_api_version("not-a-version") is None)

    kwargs = config.kafka_producer_kwargs("broker:9092", "2.5.0")
    check("producer kwargs carry the parsed api_version",
          kwargs.get("api_version") == (2, 5, 0), str(kwargs.get("api_version")))
    check("producer kwargs carry max_block_ms", "max_block_ms" in kwargs, str(kwargs))

    default_kwargs = config.kafka_producer_kwargs("broker:9092")
    check("no api_version key when unset (auto-detect)",
          "api_version" not in default_kwargs, str(default_kwargs))

    consumer_kwargs = config.kafka_consumer_kwargs("broker:9092", "group-x", "0.10.1")
    check("consumer kwargs carry the parsed api_version",
          consumer_kwargs.get("api_version") == (0, 10, 1))


# --- producer envelope ---------------------------------------------------

def test_producer_envelope() -> None:
    print("\n[producer envelope]")
    fake_kafka.reset_all_brokers()
    client = fake_kafka.FakeKafkaProducer(bootstrap_servers="unit-test-envelope")
    prod = Producer(client)

    payload = generate_payload(1)
    payload["DocRequestCode"] = "West-Bank"
    topic, partition, offset = prod.publish(payload)

    check("routed to the expected topic", topic == "bigcreditbank.applications.west-bank", topic)
    check("first message on a topic gets offset 0", offset == 0, str(offset))
    check("single-partition fake broker", partition == 0, str(partition))
    check("producer.published incremented", prod.published == 1)

    raw = client.broker._topics[topic][0]  # (offset, key, value, ts)
    check("key is the RequestId", raw[1] == payload["RequestId"])
    envelope = json.loads(raw[2])
    check("envelope carries doc_request_code", envelope["doc_request_code"] == "West-Bank")
    check("envelope carries application_no",
          envelope["application_no"] == payload["Fields"]["Main"]["ApplicationNo"])
    check("envelope payload round-trips the full document",
          envelope["payload"]["RequestId"] == payload["RequestId"])
    check("schema_version stamped", envelope["schema_version"] == "1")
    check("produced_at_ms is a recent epoch millis value",
          abs(time.time() * 1000 - envelope["produced_at_ms"]) < 5000)


# --- round trip -----------------------------------------------------------

def test_produce_consume_roundtrip() -> None:
    print("\n[produce -> consume round trip, manual commit]")
    fake_kafka.reset_all_brokers()
    addr = "unit-test-roundtrip"

    producer_client = fake_kafka.FakeKafkaProducer(bootstrap_servers=addr)
    prod = Producer(producer_client)

    codes = ["West-Bank", "East-Bank", "North-Bank", "South-Bank"]
    per_code = 3
    for code in codes:
        for seq in range(per_code):
            payload = generate_payload(seq)
            payload["DocRequestCode"] = code
            prod.publish(payload)

    check("published expected total", prod.published == len(codes) * per_code,
          str(prod.published))

    consumer_client = fake_kafka.FakeKafkaConsumer(
        *kafka_topics.all_topics(), bootstrap_servers=addr, group_id="test-group")
    cons = Consumer(consumer_client, poll_ms=10, max_records=50)

    batches = consumer_client.poll(timeout_ms=10, max_records=50)
    total_records = sum(len(records) for records in batches.values())
    check("poll returns every published record before any commit",
          total_records == len(codes) * per_code, str(total_records))

    for _tp, records in batches.items():
        cons._process_batch(records)

    check("consumer processed every record", cons.processed == len(codes) * per_code,
          str(cons.processed))
    check("consumer committed every record", cons.committed == len(codes) * per_code,
          str(cons.committed))
    check("no failures on well formed records", cons.failed == 0, str(cons.failed))

    # A second poll from the same group should see nothing new: commit advanced
    # the offset for every topic that had traffic.
    second = consumer_client.poll(timeout_ms=10, max_records=50)
    check("nothing left to read after commit", sum(len(r) for r in second.values()) == 0,
          str(second))

    for code in codes:
        topic = kafka_topics.topic_for_code(code)
        check(f"{topic} has exactly {per_code} messages",
              producer_client.broker.topic_length(topic) == per_code)


def test_consumer_handles_malformed_records() -> None:
    print("\n[consumer robustness]")
    cons = Consumer(client=None, poll_ms=10, max_records=10)

    bad_json = fake_kafka.ConsumerRecord("bigcreditbank.applications.other", 0, 0,
                                         key="k", value="not json", timestamp=time.time())
    check("malformed JSON does not raise", cons.handle(bad_json) is True)

    empty = fake_kafka.ConsumerRecord("bigcreditbank.applications.other", 0, 1,
                                      key="k", value=None, timestamp=time.time())
    check("empty value does not raise", cons.handle(empty) is True)

    payload = generate_payload(1)
    envelope = {
        "payload": payload, "request_id": payload["RequestId"],
        "doc_request_code": payload["DocRequestCode"],
        "application_no": payload["Fields"]["Main"]["ApplicationNo"],
        "agreement_no": payload["Fields"]["Main"]["AgreementNo"],
        "produced_at_ms": int(time.time() * 1000), "schema_version": "1",
    }
    good = fake_kafka.ConsumerRecord("bigcreditbank.applications.other", 0, 2,
                                     key=payload["RequestId"], value=json.dumps(envelope),
                                     timestamp=time.time())
    check("well formed record handled", cons.handle(good) is True)


# --- worker manager --------------------------------------------------------

def test_kafka_worker_manager() -> None:
    print("\n[KafkaWorkerManager lifecycle]")
    fake_kafka.reset_all_brokers()
    addr = "unit-test-workers"

    mgr = KafkaWorkerManager(Metrics(window_seconds=60))
    mgr.settings.update(bootstrap_servers=addr, rate=200)  # fast, this is a unit test
    mgr._producer_client_factory = fake_kafka.FakeKafkaProducer
    mgr._consumer_client_factory = (
        lambda topics, **kw: fake_kafka.FakeKafkaConsumer(*topics, **kw))
    mgr._topics_client_factory = fake_kafka.FakeKafkaConsumer

    ensure_calls = []

    def fake_ensure(num_partitions: int, replication_factor: int) -> tuple[bool, str]:
        ensure_calls.append((num_partitions, replication_factor))
        broker = fake_kafka.get_broker(addr)
        for t in kafka_topics.all_topics():
            broker.ensure_topic(t)
        return True, f"fake-ensured {len(kafka_topics.all_topics())} topic(s)"

    mgr._ensure_topics_fn = fake_ensure

    ok, message = mgr.ping()
    check("ping succeeds against the fake broker", ok, message)

    ok, topics_or_error = mgr.list_broker_topics()
    check("list_broker_topics succeeds before any traffic", ok, str(topics_or_error))
    check("no topics exist on a fresh broker", topics_or_error == [],
          str(topics_or_error))

    check("producer starts", mgr.start_producer())
    check("starting again is a no-op", mgr.start_producer() is False)
    check("consumer starts", mgr.start_consumer())

    produced_some = wait_for(lambda: mgr.metrics.totals()["produced"] >= 10)
    check("metrics see produced messages", produced_some, str(mgr.metrics.totals()))
    check("ensure_topics ran automatically before the first send",
          len(ensure_calls) == 1, str(ensure_calls))
    check("topics_ensured flag set so it does not run again", mgr._topics_ensured)

    consumed_some = wait_for(lambda: mgr.metrics.totals()["consumed"] >= 5)
    check("metrics see consumed messages", consumed_some, str(mgr.metrics.totals()))

    topics_snapshot = mgr.topics_status()
    any_topic_produced = any(row["produced"] > 0 for row in topics_snapshot.values())
    check("at least one topic shows produced traffic", any_topic_produced,
          str(topics_snapshot))

    ok, topics_after = mgr.list_broker_topics()
    check("list_broker_topics sees real traffic after messages were sent",
          ok and len(topics_after) > 0, str(topics_after))

    mgr.stop_all()
    stopped = wait_for(lambda: not mgr.producer_running() and not mgr.consumer_running())
    check("both workers stop", stopped)

    totals_before = mgr.metrics.totals()
    check("totals are non-zero before reset", totals_before["produced"] > 0)
    mgr.metrics.reset()
    mgr.topic_counters.reset()
    check("metrics reset to zero", mgr.metrics.totals()["produced"] == 0)
    check("topic counters reset to zero",
          all(row["produced"] == 0 for row in mgr.topics_status().values()))


def main() -> int:
    test_topic_routing()
    test_ensure_topics_exist_without_kafka_python()
    test_api_version_and_max_block_ms_config()
    test_producer_envelope()
    test_produce_consume_roundtrip()
    test_consumer_handles_malformed_records()
    test_kafka_worker_manager()

    print(f"\n{'=' * 60}")
    print(f"{PASSED} passed, {len(FAILED)} failed")
    if FAILED:
        print("Failed:")
        for label in FAILED:
            print(f"  - {label}")
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
