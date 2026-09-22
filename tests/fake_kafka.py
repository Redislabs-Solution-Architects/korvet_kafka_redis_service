"""Minimal in-memory stand-in for kafka-python's KafkaProducer/KafkaConsumer.

Only exists so kafka_producer.py / kafka_consumer.py / kafka_workers.py can be
exercised without a real Kafka broker. Not a general purpose fake, and no
substitute for running against a real cluster -- there is exactly one
partition per topic and offsets never wrap or compact.

Multiple fake clients constructed with the *same* `bootstrap_servers` string
share one in-memory `_Broker` (a module level registry keyed by that string),
the same way multiple real clients pointed at the same address share one real
cluster. That is what lets a producer thread and a consumer thread, each with
their own client instance, see each other's data in tests.
"""

from __future__ import annotations

import threading
import time
from typing import Any, Callable


class KafkaError(Exception):
    pass


class RecordMetadata:
    __slots__ = ("topic", "partition", "offset")

    def __init__(self, topic: str, partition: int, offset: int) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset


class _Future:
    """Mimics kafka-python's FutureRecordMetadata just enough for `.get()`."""

    def __init__(self, result: Any = None, error: Exception | None = None) -> None:
        self._result = result
        self._error = error

    def get(self, timeout: float | None = None) -> Any:
        if self._error is not None:
            raise self._error
        return self._result


class ConsumerRecord:
    __slots__ = ("topic", "partition", "offset", "key", "value", "timestamp")

    def __init__(self, topic: str, partition: int, offset: int,
                key: Any, value: Any, timestamp: float) -> None:
        self.topic = topic
        self.partition = partition
        self.offset = offset
        self.key = key
        self.value = value
        self.timestamp = timestamp


class TopicPartition:
    __slots__ = ("topic", "partition")

    def __init__(self, topic: str, partition: int) -> None:
        self.topic = topic
        self.partition = partition

    def __eq__(self, other: object) -> bool:
        return (isinstance(other, TopicPartition)
                and self.topic == other.topic and self.partition == other.partition)

    def __hash__(self) -> int:
        return hash((self.topic, self.partition))

    def __repr__(self) -> str:
        return f"TopicPartition(topic={self.topic!r}, partition={self.partition})"


class _Broker:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        # topic -> list of (offset, key, value, timestamp), append only
        self._topics: dict[str, list[tuple[int, Any, Any, float]]] = {}
        # (group_id, topic) -> next offset a fresh consumer in that group will read
        self._committed: dict[tuple[str, str], int] = {}

    def append(self, topic: str, key: Any, value: Any) -> int:
        with self._lock:
            log = self._topics.setdefault(topic, [])
            offset = len(log)
            log.append((offset, key, value, time.time()))
            return offset

    def fetch(self, group_id: str, topic: str,
              max_records: int) -> list[tuple[int, Any, Any, float]]:
        with self._lock:
            log = self._topics.get(topic, [])
            start = self._committed.get((group_id, topic), 0)
            return list(log[start:start + max_records])

    def commit(self, group_id: str, topic: str, next_offset: int) -> None:
        with self._lock:
            self._committed[(group_id, topic)] = next_offset

    def topic_length(self, topic: str) -> int:
        with self._lock:
            return len(self._topics.get(topic, []))

    def known_topics(self) -> set[str]:
        with self._lock:
            return set(self._topics.keys())

    def ensure_topic(self, topic: str) -> None:
        """Idempotent: create an empty topic if it does not exist yet.

        Mirrors what a real `KafkaAdminClient.create_topics()` call achieves,
        without needing kafka-python installed to test the "ensure topics
        exist on startup" feature.
        """
        with self._lock:
            self._topics.setdefault(topic, [])


_BROKERS: dict[str, _Broker] = {}
_BROKERS_LOCK = threading.Lock()


def _broker_key(bootstrap_servers: Any) -> str:
    if isinstance(bootstrap_servers, str):
        return bootstrap_servers
    return ",".join(bootstrap_servers)


def get_broker(bootstrap_servers: Any) -> _Broker:
    key = _broker_key(bootstrap_servers)
    with _BROKERS_LOCK:
        broker = _BROKERS.get(key)
        if broker is None:
            broker = _BROKERS[key] = _Broker()
        return broker


def reset_all_brokers() -> None:
    """Test isolation: call between tests that reuse a bootstrap_servers string."""
    with _BROKERS_LOCK:
        _BROKERS.clear()


class FakeKafkaProducer:
    def __init__(
        self,
        bootstrap_servers: Any = "localhost:9092",
        client_id: str | None = None,
        acks: Any = "all",
        value_serializer: Callable[[Any], Any] | None = None,
        key_serializer: Callable[[Any], Any] | None = None,
        **_kwargs: Any,
    ) -> None:
        self.broker = get_broker(bootstrap_servers)
        self.client_id = client_id
        self.acks = acks
        self._value_serializer = value_serializer or (lambda v: v)
        self._key_serializer = key_serializer or (lambda k: k)
        self.closed = False

    def send(self, topic: str, value: Any = None, key: Any = None,
             partition: int | None = None) -> _Future:
        if self.closed:
            return _Future(error=KafkaError("producer is closed"))
        v = self._value_serializer(value) if value is not None else None
        k = self._key_serializer(key) if key is not None else None
        offset = self.broker.append(topic, k, v)
        return _Future(RecordMetadata(topic, 0, offset))

    def flush(self, timeout: float | None = None) -> None:
        pass

    def bootstrap_connected(self) -> bool:
        return not self.closed

    def close(self, timeout: float | None = None) -> None:
        self.closed = True


class FakeKafkaConsumer:
    def __init__(
        self,
        *topics: str,
        bootstrap_servers: Any = "localhost:9092",
        group_id: str | None = None,
        auto_offset_reset: str = "latest",
        enable_auto_commit: bool = True,
        max_poll_records: int = 500,
        value_deserializer: Callable[[Any], Any] | None = None,
        key_deserializer: Callable[[Any], Any] | None = None,
        client_id: str | None = None,
        **_kwargs: Any,
    ) -> None:
        self.broker = get_broker(bootstrap_servers)
        # NOT named `self.topics`: real kafka-python reserves that name for the
        # `.topics()` method below (all topics on the cluster), which is a
        # different thing from what this client is subscribed to.
        self._subscribed_topics = list(topics)
        self.group_id = group_id or "fake-group"
        self.max_poll_records = max_poll_records
        self._value_deserializer = value_deserializer or (lambda v: v)
        self._key_deserializer = key_deserializer or (lambda k: k)
        self.closed = False
        self._pending_commit: dict[str, int] = {}

    def poll(self, timeout_ms: int = 0,
             max_records: int | None = None) -> dict[TopicPartition, list[ConsumerRecord]]:
        limit = max_records or self.max_poll_records
        result: dict[TopicPartition, list[ConsumerRecord]] = {}
        for topic in self._subscribed_topics:
            batch = self.broker.fetch(self.group_id, topic, limit)
            if not batch:
                continue
            records = [
                ConsumerRecord(
                    topic, 0, offset,
                    self._key_deserializer(key) if key is not None else None,
                    self._value_deserializer(value) if value is not None else None,
                    ts,
                )
                for offset, key, value, ts in batch
            ]
            result[TopicPartition(topic, 0)] = records
            self._pending_commit[topic] = batch[-1][0] + 1
        return result

    def commit(self) -> None:
        for topic, next_offset in self._pending_commit.items():
            self.broker.commit(self.group_id, topic, next_offset)
        self._pending_commit.clear()

    def topics(self) -> set[str]:
        """Every topic this fake broker has ever seen a message for."""
        return self.broker.known_topics()

    def close(self) -> None:
        self.closed = True
