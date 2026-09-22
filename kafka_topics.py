"""Kafka topic routing for the BigCreditBank application payloads.

`payloads.generate_payload()` already carries a natural category field --
`DocRequestCode` -- with a small fixed set of values. Rather than inventing an
arbitrary split, each generated document is routed to the Kafka topic that
matches its own `DocRequestCode`, so "topics" reflect a real field already in
the data instead of a synthetic partitioning scheme.

Unknown/future `DocRequestCode` values fall back to `DEFAULT_TOPIC` rather than
raising, so a broker-side schema change never take down the producer.

Every topic this module creates is given a 5 minute retention window
(`RETENTION_MS`), matching the Redis Streams side's `TTL_SECONDS`, so the two
backends age data out on the same schedule.
"""

from __future__ import annotations

import os
from typing import Any

# --- topic naming -----------------------------------------------------------

TOPIC_PREFIX = os.getenv("KAFKA_TOPIC_PREFIX", "bigcreditbank.applications")

# --- retention --------------------------------------------------------------
# 5 minutes. Kept deliberately in step with config.TTL_SECONDS so the Kafka and
# Redis Streams backends drop data at the same age.
RETENTION_MS = int(os.getenv("KAFKA_TOPIC_RETENTION_MS", "300000"))

# A broker only deletes a log segment once it is closed, so retention.ms on its
# own does very little while the active segment stays open -- Kafka's default
# segment.ms is 7 days, which would keep 5 minute old records around for a week.
# Rolling the segment on the same 5 minute cadence is what makes the retention
# window actually take effect.
#SEGMENT_MS = int(os.getenv("KAFKA_TOPIC_SEGMENT_MS", str(RETENTION_MS)))
SEGMENT_MS = int(os.getenv("KAFKA_TOPIC_SEGMENT_MS", str(RETENTION_MS // 5)))
# DocRequestCode -> topic suffix. Keys match payloads.py's
# `random.choice(["East-Bank", "West-Bank", "North-Bank", "South-Bank"])` exactly.
_DOC_CODE_SUFFIX: dict[str, str] = {
    "East-Bank": "east-bank",     # offline form
    "West-Bank": "west-bank",     # easy-apply
    "North-Bank": "north-bank",   # offline form
    "South-Bank": "south-bank",   # credit card linked
}
_DEFAULT_SUFFIX = "other"

TOPIC_MAP: dict[str, str] = {
    code: f"{TOPIC_PREFIX}.{suffix}" for code, suffix in _DOC_CODE_SUFFIX.items()
}
DEFAULT_TOPIC = f"{TOPIC_PREFIX}.{_DEFAULT_SUFFIX}"


def all_topics() -> list[str]:
    """Every topic a producer can write to / a consumer should subscribe to."""
    topics = list(dict.fromkeys(TOPIC_MAP.values()))  # de-dup, keep order
    topics.append(DEFAULT_TOPIC)
    return topics


def topic_for_code(doc_request_code: str) -> str:
    return TOPIC_MAP.get(doc_request_code, DEFAULT_TOPIC)


def topic_for_payload(payload: dict[str, Any]) -> str:
    return topic_for_code(payload.get("DocRequestCode", ""))


def topic_configs(retention_ms: int | None = None,
                  segment_ms: int | None = None) -> dict[str, str]:
    """Broker-side config applied to every topic this app creates.

    Values are strings because that is what the Kafka admin protocol carries.
    """
    return {
        "retention.ms": str(RETENTION_MS if retention_ms is None else retention_ms),
        "segment.ms": str(SEGMENT_MS if segment_ms is None else segment_ms),
    }


def set_topic_retention(
    admin: Any,
    topics: list[str],
    retention_ms: int | None = None,
    segment_ms: int | None = None,
) -> tuple[bool, str]:
    """Push the retention window onto topics that already exist.

    `create_topics()` only carries configs for topics it actually creates: a
    topic left over from an earlier run comes back "already exists" and keeps
    whatever retention it was made with. Altering the config explicitly is what
    makes a retention change apply to those, rather than only to a fresh
    broker. Best effort -- a broker with no config API, or an account without
    ALTER_CONFIGS, is reported rather than raised, since the topics are still
    usable either way.
    """
    try:
        from kafka.admin import ConfigResource, ConfigResourceType  # noqa: PLC0415
    except ImportError:
        return False, "kafka-python does not expose ConfigResource"

    wanted = topic_configs(retention_ms, segment_ms)
    try:
        resources = [ConfigResource(ConfigResourceType.TOPIC, name, wanted)
                     for name in topics]
        admin.alter_configs(resources)
        summary = ", ".join(f"{k}={v}" for k, v in wanted.items())
        return True, f"retention applied to {len(topics)} topic(s): {summary}"
    except Exception as exc:  # noqa: BLE001 - surfaced verbatim to the caller
        return False, f"{type(exc).__name__}: {exc}"


def ensure_topics_exist(
    bootstrap_servers: str,
    api_version: Any = None,
    num_partitions: int = 3,
    replication_factor: int = 1,
    topics: list[str] | None = None,
    retention_ms: int | None = None,
    segment_ms: int | None = None,
) -> tuple[bool, str]:
    """Create every topic this app uses if the broker does not already have them.

    Some Kafka-protocol brokers (this project was written against one called
    Korvet) do not auto-create a topic the first time a producer sends to it
    or a consumer polls it, the way stock Kafka can be configured to. Skipping
    this step against one of those is exactly what makes `producer.send()`
    hang until `KafkaTimeoutError: Failed to update metadata` -- the client
    keeps waiting for metadata about a topic that will never appear on its
    own. Safe to call on every startup: an "already exists" response from the
    broker is treated as success, not an error, and any other failure (no
    admin API, no permission, broker unreachable) is reported but does not
    raise, since the topics may simply already exist from a previous run or a
    manual `korvet topics --create`.

    Topics are created with a `RETENTION_MS` (5 minute) retention window, and
    the same window is then pushed onto any topic that already existed, so a
    retention change is not silently limited to brand new topics.
    """
    try:
        from kafka.admin import KafkaAdminClient, NewTopic  # noqa: PLC0415
    except ImportError:
        return False, "kafka-python is not installed"

    wanted = topics if topics is not None else all_topics()
    admin = None
    try:
        kwargs: dict[str, Any] = {"bootstrap_servers": bootstrap_servers}
        if api_version:
            kwargs["api_version"] = api_version
        admin = KafkaAdminClient(**kwargs)
        configs = topic_configs(retention_ms, segment_ms)
        specs = [NewTopic(name=t, num_partitions=num_partitions,
                          replication_factor=replication_factor,
                          topic_configs=configs) for t in wanted]
        created = True
        note = f"created (or confirmed) {len(specs)} topic(s): {', '.join(wanted)}"
        try:
            admin.create_topics(specs, validate_only=False)
        except Exception as exc:  # noqa: BLE001
            label = type(exc).__name__
            if "alreadyexists" in label.lower() or "already exists" in str(exc).lower():
                note = "topics already exist"
            else:
                raise
        # Applies to the already-exists case too, which create_topics skips.
        _, retention_note = set_topic_retention(admin, wanted, retention_ms, segment_ms)
        return created, f"{note}; {retention_note}"
    except Exception as exc:  # noqa: BLE001 - surfaced verbatim to the caller
        label = type(exc).__name__
        if "alreadyexists" in label.lower() or "already exists" in str(exc).lower():
            return True, "topics already exist"
        return False, f"{label}: {exc}"
    finally:
        if admin is not None:
            try:
                admin.close()
            except Exception:  # noqa: BLE001
                pass
