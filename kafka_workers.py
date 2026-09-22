"""Start and stop Kafka producer/consumer worker threads, instrumented into Metrics.

Kafka counterpart to workers.py's WorkerManager. Same two design rules apply:

1. **One client per thread.** The producer and consumer threads each build
   their own KafkaProducer/KafkaConsumer rather than sharing one.
2. **Per-worker stop flags.** Each loop watches its own `threading.Event`
   instead of the module level `_running` globals in kafka_producer.py /
   kafka_consumer.py, so the dashboard can stop one without the other.

On top of the aggregate Metrics (produced/consumed/acked/latency -- identical
shape to the Redis dashboard's charts), this also keeps a lightweight per-topic
breakdown (`TopicCounters`) since a single Metrics instance has no concept of
topics. That is what powers the dashboard's topic table.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

import config
from kafka_consumer import Consumer, default_consumer_client_id
from kafka_producer import Producer
from kafka_topics import all_topics, ensure_topics_exist
from metrics import Metrics
from payloads import generate_payload

log = logging.getLogger("kafka_workers")


class TopicCounters:
    """Thread-safe produced/consumed/failed counters, one row per topic."""

    def __init__(self, topics: list[str] | None = None) -> None:
        self._lock = threading.Lock()
        self._rows: dict[str, dict[str, Any]] = {}
        for t in topics or all_topics():
            self._ensure(t)

    def _ensure(self, topic: str) -> dict[str, Any]:
        row = self._rows.get(topic)
        if row is None:
            row = self._rows[topic] = {
                "produced": 0, "consumed": 0, "failed": 0,
                "last_offset": None, "last_partition": None, "last_seen": None,
            }
        return row

    def record_produced(self, topic: str) -> None:
        with self._lock:
            self._ensure(topic)["produced"] += 1

    def record_consumed(self, topic: str, partition: int, offset: int) -> None:
        with self._lock:
            row = self._ensure(topic)
            row["consumed"] += 1
            row["last_offset"] = offset
            row["last_partition"] = partition
            row["last_seen"] = time.time()

    def record_failed(self, topic: str) -> None:
        with self._lock:
            self._ensure(topic)["failed"] += 1

    def reset(self) -> None:
        with self._lock:
            for row in self._rows.values():
                row.update(produced=0, consumed=0, failed=0,
                          last_offset=None, last_partition=None, last_seen=None)

    def snapshot(self) -> dict[str, dict[str, Any]]:
        with self._lock:
            return {topic: dict(row) for topic, row in self._rows.items()}


class _InstrumentedConsumer(Consumer):
    """Consumer that reports each handled record to Metrics + TopicCounters."""

    def __init__(self, *args: Any, metrics: Metrics, topic_counters: TopicCounters,
                **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._metrics = metrics
        self._topic_counters = topic_counters

    def handle(self, record: Any) -> bool:
        envelope_lag_ms: float | None = None
        try:
            import json  # local import: keeps module import light
            envelope = json.loads(record.value) if record.value else {}
            produced_at = envelope.get("produced_at_ms")
            if produced_at:
                envelope_lag_ms = max(0.0, time.time() * 1000 - float(produced_at))
        except Exception:  # noqa: BLE001
            envelope_lag_ms = None

        result = super().handle(record)
        self._metrics.record_consumed(envelope_lag_ms)
        self._topic_counters.record_consumed(record.topic, record.partition, record.offset)
        if result:
            self._metrics.record_acked()
        else:
            self._metrics.record_failed()
            self._topic_counters.record_failed(record.topic)
        return result


class KafkaWorkerManager:
    """Owns the Kafka producer and consumer threads and the shared Metrics.

    Safe to call start/stop from a UI thread. Each start is idempotent.
    """

    def __init__(self, metrics: Metrics | None = None,
                topics: list[str] | None = None) -> None:
        self.metrics = metrics or Metrics()
        self.topic_counters = TopicCounters(topics)
        self._lock = threading.Lock()

        self._producer_thread: threading.Thread | None = None
        self._producer_stop = threading.Event()
        self._consumer_thread: threading.Thread | None = None
        self._consumer_stop = threading.Event()

        self.settings: dict[str, Any] = {
            "bootstrap_servers": config.KAFKA_BOOTSTRAP_SERVERS,
            "group": config.KAFKA_CONSUMER_GROUP,
            "client_id": "dashboard-producer-1",
            "consumer_client_id": config.KAFKA_CONSUMER_NAME or default_consumer_client_id(),
            "rate": config.KAFKA_PRODUCE_RATE,
            "acks": config.KAFKA_ACKS,
            "auto_offset_reset": config.KAFKA_AUTO_OFFSET_RESET,
            "poll_ms": config.KAFKA_POLL_MS,
            "max_records": config.KAFKA_MAX_POLL_RECORDS,
            "max_block_ms": config.KAFKA_MAX_BLOCK_MS,
            # Blank = auto-detect. Set e.g. "0.10.1" if metadata calls work but
            # produce/consume hang or time out -- see README troubleshooting.
            "api_version": config.KAFKA_API_VERSION or "",
        }

        # Test seams: swapped for fakes in the test suite.
        self._producer_client_factory: Callable[..., Any] = self._default_producer_client
        self._consumer_client_factory: Callable[..., Any] = self._default_consumer_client
        self._topics_client_factory: Callable[..., Any] = self._default_topics_client
        self._ensure_topics_fn: Callable[[int, int], tuple[bool, str]] = (
            self._default_ensure_topics)
        self._topics_ensured = False

    # --- client construction ------------------------------------------------

    @staticmethod
    def _default_producer_client(**kwargs: Any) -> Any:
        from kafka import KafkaProducer  # noqa: PLC0415
        return KafkaProducer(**kwargs)

    @staticmethod
    def _default_consumer_client(topics: list[str], **kwargs: Any) -> Any:
        from kafka import KafkaConsumer  # noqa: PLC0415
        return KafkaConsumer(*topics, **kwargs)

    @staticmethod
    def _default_topics_client(**kwargs: Any) -> Any:
        from kafka import KafkaConsumer  # noqa: PLC0415
        return KafkaConsumer(**kwargs)

    def new_producer_client(self) -> Any:
        s = self.settings
        kwargs = config.kafka_producer_kwargs(s["bootstrap_servers"], s.get("api_version"))
        kwargs.update(client_id=s["client_id"], acks=s["acks"],
                     max_block_ms=s.get("max_block_ms", config.KAFKA_MAX_BLOCK_MS))
        return self._producer_client_factory(**kwargs)

    def new_consumer_client(self) -> Any:
        s = self.settings
        kwargs = config.kafka_consumer_kwargs(s["bootstrap_servers"], s["group"],
                                              s.get("api_version"))
        kwargs.update(client_id=s["consumer_client_id"],
                     auto_offset_reset=s["auto_offset_reset"],
                     max_poll_records=s["max_records"])
        return self._consumer_client_factory(all_topics(), **kwargs)

    def _default_ensure_topics(self, num_partitions: int, replication_factor: int) -> tuple[bool, str]:
        version = config.parse_api_version(self.settings.get("api_version"))
        return ensure_topics_exist(self.settings["bootstrap_servers"], version,
                                   num_partitions, replication_factor)

    def ensure_topics(self, num_partitions: int = 3, replication_factor: int = 1) -> tuple[bool, str]:
        """Create this app's topics on the broker if they are not there already.

        Called automatically once before the producer's first send() (see
        `_producer_loop`), and also exposed for a manual "Create topics"
        button in the dashboards. See kafka_topics.ensure_topics_exist() for
        why this exists: some brokers (Korvet included, depending on how it
        is configured) do not auto-create a topic on first use.
        """
        ok, message = self._ensure_topics_fn(num_partitions, replication_factor)
        if ok:
            self._topics_ensured = True
        return ok, message

    def new_topics_client(self) -> Any:
        """A bare client with no subscription, just for metadata (topics/ping)."""
        kwargs: dict[str, Any] = {"bootstrap_servers": self.settings["bootstrap_servers"]}
        version = config.parse_api_version(self.settings.get("api_version"))
        if version:
            kwargs["api_version"] = version
        return self._topics_client_factory(**kwargs)

    def ping(self) -> tuple[bool, str]:
        """Check the broker from the UI thread. Returns (ok, message).

        Deliberately does *not* build a KafkaProducer and check
        `bootstrap_connected()`: kafka-python's producer connects to its
        bootstrap seed node asynchronously via a background Sender thread, so
        that flag can still read False for a moment right after construction
        even when the broker is perfectly reachable -- and once the client has
        pulled real metadata, it may drop the seed connection entirely, so the
        flag can read False forever after that even though everything works.
        Fetching topic metadata (the same call list_broker_topics() makes) is
        a synchronous round trip and is what actually proves connectivity.
        """
        client = None
        try:
            client = self.new_topics_client()
            topics = client.topics()
            return True, (f"connected to {self.settings['bootstrap_servers']} "
                         f"({len(topics)} topic(s) visible)")
        except Exception as exc:  # noqa: BLE001 - surfaced verbatim in the UI
            return False, f"{type(exc).__name__}: {exc}"
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:  # noqa: BLE001
                    pass

    def list_broker_topics(self) -> tuple[bool, Any]:
        """Every topic the broker knows about, not just the ones this app uses.

        The code equivalent of `korvet topics --list --bootstrap-server ...` (or
        `kafka-topics.sh --list`): builds a bare client with no subscription and
        reads cluster metadata. Returns (ok, sorted list of topic names) or
        (False, error message).
        """
        client = None
        try:
            client = self.new_topics_client()
            topics = sorted(client.topics())
            return True, topics
        except Exception as exc:  # noqa: BLE001 - surfaced verbatim in the UI
            return False, f"{type(exc).__name__}: {exc}"
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:  # noqa: BLE001
                    pass

    # --- producer ------------------------------------------------------------

    def producer_running(self) -> bool:
        t = self._producer_thread
        return t is not None and t.is_alive()

    def start_producer(self) -> bool:
        with self._lock:
            if self.producer_running():
                return False
            self._producer_stop.clear()
            self._producer_thread = threading.Thread(
                target=self._producer_loop, name="kafka-producer", daemon=True)
            self._producer_thread.start()
            self.metrics.mark_producer_started()
            return True

    def stop_producer(self, timeout: float = 5.0) -> bool:
        self._producer_stop.set()
        t = self._producer_thread
        if t is not None and t.is_alive():
            t.join(timeout=timeout)
        self.metrics.mark_producer_stopped()
        return not (t is not None and t.is_alive())

    def _producer_loop(self) -> None:
        s = self.settings
        client = None
        try:
            if not self._topics_ensured:
                ok, message = self.ensure_topics()
                (log.info if ok else log.warning)("ensure topics: %s", message)
                if not ok:
                    self.metrics.record_error(f"could not ensure topics exist: {message}")
            client = self.new_producer_client()
            prod = Producer(client)
            rate = float(s["rate"])
            interval = 1.0 / rate if rate > 0 else 0.0
            seq = 0
            next_tick = time.monotonic()

            while not self._producer_stop.is_set():
                seq += 1
                try:
                    _topic, partition, offset = prod.publish(generate_payload(seq))
                    self.metrics.record_produced()
                    self.topic_counters.record_produced(_topic)
                except Exception as exc:  # noqa: BLE001
                    self.metrics.record_error(f"producer send failed: {exc}")
                    if self._producer_stop.wait(1.0):
                        break
                    continue

                next_tick += interval
                sleep_for = next_tick - time.monotonic()
                if sleep_for > 0:
                    if self._producer_stop.wait(sleep_for):
                        break
                else:
                    next_tick = time.monotonic()
        except Exception as exc:  # noqa: BLE001
            self.metrics.record_error(f"producer thread died: {type(exc).__name__}: {exc}")
            log.exception("producer thread died")
        finally:
            if client is not None:
                try:
                    client.flush(timeout=5)
                    client.close()
                except Exception:  # noqa: BLE001
                    pass
            self.metrics.mark_producer_stopped()

    # --- consumer ------------------------------------------------------------

    def consumer_running(self) -> bool:
        t = self._consumer_thread
        return t is not None and t.is_alive()

    def start_consumer(self) -> bool:
        with self._lock:
            if self.consumer_running():
                return False
            self._consumer_stop.clear()
            self._consumer_thread = threading.Thread(
                target=self._consumer_loop, name="kafka-consumer", daemon=True)
            self._consumer_thread.start()
            self.metrics.mark_consumer_started()
            return True

    def stop_consumer(self, timeout: float = 10.0) -> bool:
        self._consumer_stop.set()
        t = self._consumer_thread
        if t is not None and t.is_alive():
            t.join(timeout=timeout)
        self.metrics.mark_consumer_stopped()
        return not (t is not None and t.is_alive())

    def _consumer_loop(self) -> None:
        s = self.settings
        client = None
        try:
            client = self.new_consumer_client()
            cons = _InstrumentedConsumer(
                client, s["poll_ms"], s["max_records"],
                metrics=self.metrics, topic_counters=self.topic_counters,
            )

            while not self._consumer_stop.is_set():
                try:
                    batches = client.poll(timeout_ms=s["poll_ms"],
                                          max_records=s["max_records"])
                except Exception as exc:  # noqa: BLE001
                    self.metrics.record_error(f"consumer poll failed: {exc}")
                    if self._consumer_stop.wait(1.0):
                        break
                    continue

                if not batches:
                    continue
                for _tp, records in batches.items():
                    cons._process_batch(records)
        except Exception as exc:  # noqa: BLE001
            self.metrics.record_error(f"consumer thread died: {type(exc).__name__}: {exc}")
            log.exception("consumer thread died")
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:  # noqa: BLE001
                    pass
            self.metrics.mark_consumer_stopped()

    # --- combined --------------------------------------------------------

    def stop_all(self) -> None:
        self.stop_producer()
        self.stop_consumer()

    def status(self) -> dict[str, Any]:
        return {
            "producer_running": self.producer_running(),
            "consumer_running": self.consumer_running(),
            "settings": dict(self.settings),
            "topics": all_topics(),
        }

    def topics_status(self) -> dict[str, dict[str, Any]]:
        return self.topic_counters.snapshot()
