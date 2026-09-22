"""Shared configuration for the Redis stream producer and consumer.

Every value can be overridden with an environment variable, and the producer and
consumer both expose the same knobs as CLI flags (flags win over env vars).
"""

from __future__ import annotations

import os

# --- Connection ------------------------------------------------------------

REDIS_HOST = os.getenv("REDIS_HOST", "127.0.0.1")
REDIS_PORT = int(os.getenv("REDIS_PORT", "6379"))
REDIS_DB = int(os.getenv("REDIS_DB", "0"))
REDIS_USERNAME = os.getenv("REDIS_USERNAME") or None
REDIS_PASSWORD = os.getenv("REDIS_PASSWORD") or None

# TLS. Set REDIS_SSL=1 for a rediss:// style endpoint. REDIS_SSL_VERIFY=0 accepts
# a self-signed certificate, which the PoC cluster uses.
REDIS_SSL = os.getenv("REDIS_SSL", "0") in ("1", "true", "True", "yes")
REDIS_SSL_VERIFY = os.getenv("REDIS_SSL_VERIFY", "1") not in ("0", "false", "False", "no")
REDIS_SSL_CA_CERTS = os.getenv("REDIS_SSL_CA_CERTS") or None

# --- Stream topology -------------------------------------------------------

STREAM_KEY = os.getenv("STREAM_KEY", "bigcreditbank:applications")
CONSUMER_GROUP = os.getenv("CONSUMER_GROUP", "doc-processors")

# --- Retention -------------------------------------------------------------
# Redis Streams have no per-message expiry, so "TTL" is implemented as an age
# based trim: entries whose ID timestamp is older than TTL_SECONDS are removed
# with XTRIM MINID. See README for the full explanation.

TTL_SECONDS = int(os.getenv("TTL_SECONDS", "300"))  # 5 minutes

# Safety net so a stalled consumer cannot let the stream grow without bound.
# Set to 0 to disable and rely on age based trimming alone.
MAX_STREAM_LEN = int(os.getenv("MAX_STREAM_LEN", "100000"))

# How often the producer runs the trim, in seconds.
TRIM_INTERVAL_SECONDS = float(os.getenv("TRIM_INTERVAL_SECONDS", "10"))

# --- Producer --------------------------------------------------------------

# Messages per second. Fractional values are fine (0.5 = one every 2s).
PRODUCE_RATE = float(os.getenv("PRODUCE_RATE", "5"))

# 0 means run until interrupted.
PRODUCE_COUNT = int(os.getenv("PRODUCE_COUNT", "0"))

# --- Consumer --------------------------------------------------------------

CONSUMER_NAME = os.getenv("CONSUMER_NAME", "")  # blank -> auto: host-pid
BATCH_SIZE = int(os.getenv("BATCH_SIZE", "10"))
BLOCK_MS = int(os.getenv("BLOCK_MS", "5000"))

# Pending entries idle longer than this are reclaimed via XAUTOCLAIM.
CLAIM_MIN_IDLE_MS = int(os.getenv("CLAIM_MIN_IDLE_MS", "60000"))
CLAIM_INTERVAL_SECONDS = float(os.getenv("CLAIM_INTERVAL_SECONDS", "30"))

# --- Kafka (topic based producer/consumer simulation) ----------------------
# Separate from the Redis Streams settings above: this app can run either
# backend. Kafka needs a real broker reachable at KAFKA_BOOTSTRAP_SERVERS --
# there is no in-process fallback for kafka_producer.py / kafka_consumer.py.

# Topic retention lives in kafka_topics.py (RETENTION_MS), set to the same
# 5 minute window as TTL_SECONDS above.
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "localhost:9092")
KAFKA_CLIENT_ID = os.getenv("KAFKA_CLIENT_ID", "bigcreditbank-app-producer")
KAFKA_CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "bigcreditbank-doc-processors")
KAFKA_CONSUMER_NAME = os.getenv("KAFKA_CONSUMER_NAME", "")  # blank -> auto: host-pid

# Messages per second and stop-after-N, same semantics as PRODUCE_RATE/COUNT.
KAFKA_PRODUCE_RATE = float(os.getenv("KAFKA_PRODUCE_RATE", "5"))
KAFKA_PRODUCE_COUNT = int(os.getenv("KAFKA_PRODUCE_COUNT", "0"))

# How long a single poll() call blocks waiting for records, and how many it
# returns at most per topic-partition.
KAFKA_POLL_MS = int(os.getenv("KAFKA_POLL_MS", "2000"))
KAFKA_MAX_POLL_RECORDS = int(os.getenv("KAFKA_MAX_POLL_RECORDS", "50"))

# --- Fetch shaping ---------------------------------------------------------
# These two decide whether the broker answers a Fetch the moment a record
# lands, or holds the request and answers on a cycle.
#
# kafka-python's defaults (1 byte / 500 ms) ask for the first behaviour, and
# Kafka honours it: fetch_min_bytes=1 is satisfied by the first append, so the
# parked request completes immediately. That is why Kafka shows a ~16ms lag
# here while a broker that serves fetches on a cycle shows ~100ms at the same
# throughput.
#
# To make Kafka deliver on a cycle instead -- useful for a like-for-like
# comparison against such a broker -- raise KAFKA_FETCH_MIN_BYTES high enough
# that it cannot be met inside the wait, and set KAFKA_FETCH_MAX_WAIT_MS to
# the cycle length you want. Every fetch then returns when the wait expires
# rather than on arrival, so a record waits uniformly in [0, wait] and the
# median lag lands near wait/2.
#
KAFKA_FETCH_MIN_BYTES=1048576 
KAFKA_FETCH_MAX_WAIT_MS=200
#
# Sizing note: the gate is total bytes across the whole fetch response, so it
# has to beat rate x record size x wait. At ~165 msg/s of ~4.5 KB records that
# is ~742 KB/s, i.e. ~148 KB per 200 ms, so 1 MB is comfortably unreachable
# while staying under fetch_max_bytes (50 MB by default). Also keep
# KAFKA_MAX_POLL_RECORDS above rate x wait (~33 records at 200 ms) or the
# clump is truncated and the next poll returns at once, defeating the point.
KAFKA_FETCH_MIN_BYTES = int(os.getenv("KAFKA_FETCH_MIN_BYTES", "1048576"))
KAFKA_FETCH_MAX_WAIT_MS = int(os.getenv("KAFKA_FETCH_MAX_WAIT_MS", "200"))

# "all" waits for every in-sync replica to ack; safest, still fine for a demo
# at this volume. Use "1" for lower latency if durability does not matter.
KAFKA_ACKS = os.getenv("KAFKA_ACKS", "all")
KAFKA_AUTO_OFFSET_RESET = os.getenv("KAFKA_AUTO_OFFSET_RESET", "earliest")

# How long send() blocks waiting for topic metadata before raising
# KafkaTimeoutError. kafka-python's own default is 60000ms, which is a long
# time to sit blocked in a demo loop when something is actually wrong (topic
# auto-creation disabled, an advertised listener the client cannot reach, a
# broker that only speaks a protocol subset kafka-python's auto-negotiation
# does not land on). 10s surfaces that as a metrics error quickly instead.
KAFKA_MAX_BLOCK_MS = int(os.getenv("KAFKA_MAX_BLOCK_MS", "10000"))

# Pin the wire protocol version instead of letting kafka-python auto-detect
# it by probing the broker. Auto-detection is the first thing to try
# disabling against a broker that only implements a subset of the Kafka
# protocol (Korvet-like servers, most test/toy brokers): leave this blank for
# auto-detect, or set e.g. "0.10.1" / "2.5.0" if metadata calls work but
# produce/consume hang or time out. See README "Kafka topics" troubleshooting.
KAFKA_API_VERSION = os.getenv("KAFKA_API_VERSION", "") or None


def parse_api_version(value: str | None) -> tuple[int, ...] | None:
    if not value:
        return None
    try:
        return tuple(int(part) for part in value.split("."))
    except ValueError:
        return None

# --- Logging ---------------------------------------------------------------

LOG_LEVEL = os.getenv("LOG_LEVEL", "INFO").upper()


def redis_kwargs() -> dict:
    """Keyword arguments for redis_client.Redis(...).

    decode_responses=True keeps stream field/value handling as str rather than
    bytes, which matters because the payload is JSON.
    """
    kwargs: dict = {
        "host": REDIS_HOST,
        "port": REDIS_PORT,
        "db": REDIS_DB,
        "decode_responses": True,
        "socket_keepalive": True,
        "socket_connect_timeout": 10.0,
        # Blocking reads set their own deadline from the BLOCK argument, so the
        # default socket timeout only bounds ordinary commands.
        "socket_timeout": 30.0,
    }
    if REDIS_USERNAME:
        kwargs["username"] = REDIS_USERNAME
    if REDIS_PASSWORD:
        kwargs["password"] = REDIS_PASSWORD
    if REDIS_SSL:
        kwargs["ssl"] = True
        kwargs["ssl_cert_reqs"] = "required" if REDIS_SSL_VERIFY else "none"
        if REDIS_SSL_CA_CERTS:
            kwargs["ssl_ca_certs"] = REDIS_SSL_CA_CERTS
    return kwargs


def kafka_producer_kwargs(bootstrap_servers: str | None = None,
                          api_version: str | None = None) -> dict:
    """Keyword arguments for `kafka.KafkaProducer(...)`."""
    kwargs = {
        "bootstrap_servers": bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS,
        "client_id": KAFKA_CLIENT_ID,
        "acks": KAFKA_ACKS,
        "max_block_ms": KAFKA_MAX_BLOCK_MS,
        "value_serializer": lambda v: v if isinstance(v, bytes) else str(v).encode("utf-8"),
        "key_serializer": lambda k: k if (k is None or isinstance(k, bytes)) else str(k).encode("utf-8"),
    }
    version = parse_api_version(api_version if api_version is not None else KAFKA_API_VERSION)
    if version:
        kwargs["api_version"] = version
    return kwargs


def kafka_consumer_kwargs(bootstrap_servers: str | None = None,
                          group_id: str | None = None,
                          api_version: str | None = None) -> dict:
    """Keyword arguments for `kafka.KafkaConsumer(...)`."""
    kwargs = {
        "bootstrap_servers": bootstrap_servers or KAFKA_BOOTSTRAP_SERVERS,
        "group_id": group_id or KAFKA_CONSUMER_GROUP,
        "auto_offset_reset": KAFKA_AUTO_OFFSET_RESET,
        "enable_auto_commit": False,  # commit only after handle() succeeds
        "max_poll_records": KAFKA_MAX_POLL_RECORDS,
        "fetch_min_bytes": KAFKA_FETCH_MIN_BYTES,
        "fetch_max_wait_ms": KAFKA_FETCH_MAX_WAIT_MS,
        "value_deserializer": lambda v: v.decode("utf-8") if v is not None else None,
        "key_deserializer": lambda k: k.decode("utf-8") if k is not None else None,
    }
    version = parse_api_version(api_version if api_version is not None else KAFKA_API_VERSION)
    if version:
        kwargs["api_version"] = version
    return kwargs
