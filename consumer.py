#!/usr/bin/env python3
"""Redis Streams consumer for BigCreditBank application payloads.

Joins a consumer group, blocks on XREADGROUP, processes each entry and XACKs it.
Entries left pending by a crashed consumer are reclaimed with XAUTOCLAIM so no
message is stranded.

Run with --help for the full flag list.
"""

from __future__ import annotations

import argparse
import json
import logging
import os
import signal
import socket
import sys
import time
from typing import Any

import config
import redis_client as redis
from redis_client import (
    ConnectionError as RedisConnectionError,
    RedisError,
    ResponseError,
)

log = logging.getLogger("consumer")

_running = True


def _handle_signal(signum: int, _frame: Any) -> None:
    global _running
    log.info("received %s, finishing current batch then stopping",
             signal.Signals(signum).name)
    _running = False


def _install_signal_handlers() -> None:
    """Register handlers, unless we are embedded in a non-main thread."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_signal)
        except ValueError:
            log.debug("not the main thread, skipping %s handler", sig.name)


def default_consumer_name() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


class Consumer:
    """Reads, processes and acknowledges entries as part of a consumer group."""

    def __init__(
        self,
        client: redis.Redis,
        stream: str,
        group: str,
        name: str,
        batch_size: int,
        block_ms: int,
        claim_min_idle_ms: int,
        claim_interval: float,
    ) -> None:
        self.client = client
        self.stream = stream
        self.group = group
        self.name = name
        self.batch_size = batch_size
        self.block_ms = block_ms
        self.claim_min_idle_ms = claim_min_idle_ms
        self.claim_interval = claim_interval

        self._claim_cursor = "0-0"
        self._last_claim = 0.0
        self.processed = 0
        self.acked = 0
        self.failed = 0

    # --- setup -------------------------------------------------------------

    def ensure_group(self) -> None:
        """Create the group, tolerating the case where it already exists.

        mkstream=True means the consumer can start before the producer and the
        group survives; id="0" so a group created after messages already exist
        still sees the backlog inside the retention window.
        """
        try:
            self.client.xgroup_create(self.stream, self.group, id="0", mkstream=True)
            log.info("created consumer group %r on %r", self.group, self.stream)
        except ResponseError as exc:
            if "BUSYGROUP" in str(exc):
                log.info("consumer group %r already exists on %r", self.group, self.stream)
            else:
                raise

    # --- processing --------------------------------------------------------

    def handle(self, entry_id: str, fields: dict[str, str]) -> bool:
        """Process a single entry. Return True to ack, False to leave pending.

        Replace the body of this method with the real downstream work. Returning
        False deliberately leaves the entry in the Pending Entries List so it
        gets retried by XAUTOCLAIM rather than silently dropped.
        """
        raw = fields.get("payload")
        if not raw:
            log.warning("%s has no payload field, acking to avoid a poison loop", entry_id)
            return True

        try:
            payload = json.loads(raw)
        except json.JSONDecodeError as exc:
            log.error("%s payload is not valid JSON (%s), acking as unprocessable",
                      entry_id, exc)
            return True

        main = payload["Fields"]["Main"]
        app = payload["Fields"]["App"]
        finance = payload["Fields"]["Finance"]
        products = [p for p in payload["Fields"]["Product"] if p["Brand"]]

        # Latency from XADD to here, which is the number worth watching.
        produced_at = fields.get("produced_at_ms")
        lag_ms = int(time.time() * 1000) - int(produced_at) if produced_at else -1

        log.info(
            "ACK %s | %s | %s %s | %s | %d item(s) | RM%s over %sm @ RM%s | lag %dms",
            entry_id,
            main["ApplicationNo"],
            app["Title"],
            app["FullName"],
            payload["DocRequestCode"],
            len(products),
            finance["FinanceAmount"],
            finance["Months"],
            finance["InstalmentAmt"],
            lag_ms,
        )
        return True

    def _process_batch(self, entries: list[tuple[str, dict[str, str]]], source: str) -> None:
        to_ack: list[str] = []
        for entry_id, fields in entries:
            # A reclaimed entry whose data was trimmed away comes back as None.
            if fields is None:
                to_ack.append(entry_id)
                continue
            self.processed += 1
            try:
                if self.handle(entry_id, fields):
                    to_ack.append(entry_id)
                else:
                    self.failed += 1
                    log.warning("%s left pending for retry", entry_id)
            except Exception:  # noqa: BLE001 - one bad entry must not kill the loop
                self.failed += 1
                log.exception("%s raised during handling, leaving pending", entry_id)

        if to_ack:
            # Acking the batch in one call rather than per entry keeps the PEL
            # churn and the round trips down.
            self.client.xack(self.stream, self.group, *to_ack)
            self.acked += len(to_ack)
            log.debug("acked %d entries from %s", len(to_ack), source)

    # --- recovery ----------------------------------------------------------

    def reclaim_stale(self) -> None:
        """Take over entries another consumer read but never acked."""
        now = time.monotonic()
        if (now - self._last_claim) < self.claim_interval:
            return
        self._last_claim = now

        try:
            result = self.client.xautoclaim(
                self.stream,
                self.group,
                self.name,
                min_idle_time=self.claim_min_idle_ms,
                start_id=self._claim_cursor,
                count=self.batch_size,
            )
        except RedisError as exc:
            log.warning("XAUTOCLAIM failed: %s", exc)
            return

        # Redis 7.0+ returns [cursor, entries, deleted]; 6.2 returns [cursor, entries].
        if len(result) == 3:
            cursor, entries, _deleted = result
        else:
            cursor, entries = result

        self._claim_cursor = cursor if cursor != "0-0" else "0-0"
        if entries:
            log.info("reclaimed %d stale pending entr%s",
                     len(entries), "y" if len(entries) == 1 else "ies")
            self._process_batch(entries, "xautoclaim")

    # --- main loop ---------------------------------------------------------

    def run(self) -> None:
        log.info("consuming %r as %s/%s (batch=%d, block=%dms)",
                 self.stream, self.group, self.name, self.batch_size, self.block_ms)

        while _running:
            try:
                self.reclaim_stale()

                response = self.client.xreadgroup(
                    groupname=self.group,
                    consumername=self.name,
                    # ">" means: only entries never delivered to this group.
                    streams={self.stream: ">"},
                    count=self.batch_size,
                    block=self.block_ms,
                )
            except RedisConnectionError as exc:
                log.error("connection lost, retrying in 2s: %s", exc)
                time.sleep(2)
                continue
            except ResponseError as exc:
                # The group can vanish if someone deletes the stream underneath us.
                if "NOGROUP" in str(exc):
                    log.warning("group disappeared, recreating: %s", exc)
                    self.ensure_group()
                    continue
                log.error("XREADGROUP failed: %s", exc)
                time.sleep(1)
                continue

            if not response:
                log.debug("idle, no new entries in %dms", self.block_ms)
                continue

            for _stream_name, entries in response:
                self._process_batch(entries, "xreadgroup")

        log.info("stopped: processed=%d acked=%d failed=%d",
                 self.processed, self.acked, self.failed)

    # --- introspection -----------------------------------------------------

    def pending_count(self) -> int:
        try:
            return int(self.client.xpending(self.stream, self.group)["pending"])
        except (RedisError, KeyError, TypeError):
            return -1


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Consume and acknowledge application payloads from a Redis stream.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--host", default=config.REDIS_HOST)
    p.add_argument("--port", type=int, default=config.REDIS_PORT)
    p.add_argument("--db", type=int, default=config.REDIS_DB)
    p.add_argument("--password", default=config.REDIS_PASSWORD)
    p.add_argument("--username", default=config.REDIS_USERNAME)
    p.add_argument("--tls", action="store_true", default=config.REDIS_SSL,
                   help="connect with TLS")
    p.add_argument("--tls-no-verify", action="store_true",
                   default=not config.REDIS_SSL_VERIFY,
                   help="accept a self-signed server certificate")
    p.add_argument("--stream", default=config.STREAM_KEY)
    p.add_argument("--group", default=config.CONSUMER_GROUP)
    p.add_argument("--name", default=config.CONSUMER_NAME or default_consumer_name(),
                   help="consumer name, must be unique within the group")
    p.add_argument("--batch-size", type=int, default=config.BATCH_SIZE)
    p.add_argument("--block-ms", type=int, default=config.BLOCK_MS,
                   help="how long XREADGROUP blocks waiting for new entries")
    p.add_argument("--claim-min-idle-ms", type=int, default=config.CLAIM_MIN_IDLE_MS,
                   help="reclaim pending entries idle longer than this")
    p.add_argument("--claim-interval", type=float, default=config.CLAIM_INTERVAL_SECONDS,
                   help="seconds between XAUTOCLAIM passes")
    p.add_argument("--log-level", default=config.LOG_LEVEL)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    _install_signal_handlers()

    kwargs = config.redis_kwargs()
    kwargs.update(host=args.host, port=args.port, db=args.db)
    if args.password:
        kwargs["password"] = args.password
    if args.username:
        kwargs["username"] = args.username
    if args.tls:
        kwargs["ssl"] = True
        kwargs["ssl_cert_reqs"] = "none" if args.tls_no_verify else "required"
    client = redis.Redis(**kwargs)

    try:
        client.ping()
    except RedisError as exc:
        log.error("cannot reach Redis at %s:%s -- %s", args.host, args.port, exc)
        return 1
    log.info("connected to Redis at %s:%s db=%s", args.host, args.port, args.db)

    consumer = Consumer(
        client, args.stream, args.group, args.name,
        args.batch_size, args.block_ms,
        args.claim_min_idle_ms, args.claim_interval,
    )
    try:
        consumer.ensure_group()
        consumer.run()
    finally:
        log.info("pending entries left in group: %d", consumer.pending_count())
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
