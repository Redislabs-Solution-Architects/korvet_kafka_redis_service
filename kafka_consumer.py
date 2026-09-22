#!/usr/bin/env python3
"""Kafka consumer for bigcreditbank application payloads.

Joins a consumer group and subscribes to every topic in kafka_topics.all_topics()
(one topic per DocRequestCode), polls in batches, processes each record and
commits offsets only after the batch is handled -- the Kafka analogue of the
Redis Streams consumer's XREADGROUP + XACK.

Requires a reachable Kafka broker -- there is no in-process fallback. Run with
--help for the full flag list.
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
from kafka_topics import all_topics, ensure_topics_exist

log = logging.getLogger("kafka_consumer")

_running = True


def _handle_signal(signum: int, _frame: Any) -> None:
    global _running
    log.info("received %s, finishing current batch then stopping",
             signal.Signals(signum).name)
    _running = False


def _install_signal_handlers() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_signal)
        except ValueError:
            log.debug("not the main thread, skipping %s handler", sig.name)


def default_consumer_client_id() -> str:
    return f"{socket.gethostname()}-{os.getpid()}"


def _build_client(topics: list[str], **kwargs: Any) -> Any:
    """Import kafka-python lazily so fakes can be injected without it installed."""
    from kafka import KafkaConsumer  # noqa: PLC0415
    return KafkaConsumer(*topics, **kwargs)


class Consumer:
    """Polls, processes and commits offsets for every subscribed topic."""

    def __init__(
        self,
        client: Any,
        poll_ms: int,
        max_records: int,
    ) -> None:
        self.client = client
        self.poll_ms = poll_ms
        self.max_records = max_records

        self.processed = 0
        self.committed = 0
        self.failed = 0

    # --- processing ------------------------------------------------------

    def handle(self, record: Any) -> bool:
        """Process a single record. Return True to count it as committed.

        Replace the body of this method with real downstream work. This
        mirrors consumer.py's Consumer.handle() so the two backends log the
        same shape of information.
        """
        raw = record.value
        if not raw:
            log.warning("%s-%d@%d has no value, skipping", record.topic,
                       record.partition, record.offset)
            return True

        try:
            envelope = json.loads(raw)
        except json.JSONDecodeError as exc:
            log.error("%s-%d@%d value is not valid JSON (%s), skipping as unprocessable",
                     record.topic, record.partition, record.offset, exc)
            return True

        payload = envelope.get("payload", envelope)
        main = payload["Fields"]["Main"]
        app = payload["Fields"]["App"]
        finance = payload["Fields"]["Finance"]
        products = [p for p in payload["Fields"]["Product"] if p["Brand"]]

        produced_at = envelope.get("produced_at_ms")
        lag_ms = int(time.time() * 1000) - int(produced_at) if produced_at else -1

        log.info(
            "COMMIT %s-%d@%d | %s | %s %s | %s | %d item(s) | RM%s over %sm @ RM%s | lag %dms",
            record.topic, record.partition, record.offset,
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

    def _process_batch(self, records: list[Any]) -> None:
        any_failed = False
        for record in records:
            self.processed += 1
            try:
                if not self.handle(record):
                    self.failed += 1
                    any_failed = True
                    log.warning("%s-%d@%d handler returned False, offset still advances",
                               record.topic, record.partition, record.offset)
            except Exception:  # noqa: BLE001 - one bad record must not kill the loop
                self.failed += 1
                any_failed = True
                log.exception("%s-%d@%d raised during handling", record.topic,
                              record.partition, record.offset)

        try:
            self.client.commit()
            self.committed += len(records)
        except Exception as exc:  # noqa: BLE001
            log.warning("commit failed: %s", exc)
        if any_failed:
            log.debug("batch had failures but offsets were still committed "
                     "(no PEL/XAUTOCLAIM equivalent in this simulation)")

    # --- main loop ---------------------------------------------------------

    def run(self) -> None:
        log.info("consuming topics=%s (max_records=%d, poll=%dms)",
                 all_topics(), self.max_records, self.poll_ms)

        while _running:
            try:
                batches = self.client.poll(timeout_ms=self.poll_ms,
                                           max_records=self.max_records)
            except Exception as exc:  # noqa: BLE001
                log.error("poll failed, retrying in 2s: %s", exc)
                time.sleep(2)
                continue

            if not batches:
                log.debug("idle, no new records in %dms", self.poll_ms)
                continue

            for _topic_partition, records in batches.items():
                self._process_batch(records)

        log.info("stopped: processed=%d committed=%d failed=%d",
                 self.processed, self.committed, self.failed)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Consume and commit application payloads from Kafka topics.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS,
                   help="comma separated host:port list")
    p.add_argument("--group", default=config.KAFKA_CONSUMER_GROUP)
    p.add_argument("--client-id", default=config.KAFKA_CONSUMER_NAME or default_consumer_client_id())
    p.add_argument("--max-records", type=int, default=config.KAFKA_MAX_POLL_RECORDS)
    p.add_argument("--poll-ms", type=int, default=config.KAFKA_POLL_MS)
    p.add_argument("--auto-offset-reset", default=config.KAFKA_AUTO_OFFSET_RESET)
    p.add_argument("--api-version", default=config.KAFKA_API_VERSION,
                   help="pin the wire protocol version, e.g. 0.10.1 -- try this "
                        "if metadata calls work but poll() hangs or times out "
                        "against a broker that only implements part of the "
                        "Kafka protocol")
    p.add_argument("--no-ensure-topics", action="store_true",
                   help="skip auto-creating this app's topics on startup")
    p.add_argument("--log-level", default=config.LOG_LEVEL)
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    logging.basicConfig(
        level=args.log_level.upper(),
        format="%(asctime)s %(levelname)-7s %(name)s | %(message)s",
    )
    _install_signal_handlers()

    kwargs = config.kafka_consumer_kwargs(args.bootstrap_servers, args.group, args.api_version)
    kwargs.update(client_id=args.client_id, auto_offset_reset=args.auto_offset_reset,
                 max_poll_records=args.max_records)
    try:
        client = _build_client(all_topics(), **kwargs)
    except ImportError:
        log.error("kafka-python is not installed -- run: pip install kafka-python")
        return 1
    except Exception as exc:  # noqa: BLE001
        log.error("cannot reach Kafka at %s -- %s", args.bootstrap_servers, exc)
        return 1
    log.info("connected to Kafka at %s, group=%s", args.bootstrap_servers, args.group)

    if not args.no_ensure_topics:
        ok, message = ensure_topics_exist(args.bootstrap_servers,
                                          config.parse_api_version(args.api_version))
        (log.info if ok else log.warning)("ensure topics: %s", message)

    consumer = Consumer(client, args.poll_ms, args.max_records)
    try:
        consumer.run()
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
