#!/usr/bin/env python3
"""Kafka producer for randomised bigcreditbank application payloads.

Same payload generator as the Redis Streams producer.py (`payloads.generate_payload`),
but published to Kafka topics instead of a single Redis stream: each document
is routed to the topic that matches its own `DocRequestCode` (see
kafka_topics.py), so a downstream consumer can subscribe to just the document
types it cares about.

Requires a reachable Kafka broker -- there is no in-process fallback. Run with
--help for the full flag list.

    python kafka_producer.py --bootstrap-servers localhost:9092 --rate 5
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from typing import Any, Callable

import config
from kafka_topics import ensure_topics_exist, topic_for_payload
from payloads import generate_payload

log = logging.getLogger("kafka_producer")

_running = True


def _handle_signal(signum: int, _frame: Any) -> None:
    global _running
    log.info("received %s, finishing current tick then stopping",
             signal.Signals(signum).name)
    _running = False


def _install_signal_handlers() -> None:
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_signal)
        except ValueError:
            log.debug("not the main thread, skipping %s handler", sig.name)


def _build_client(**kwargs: Any) -> Any:
    """Import kafka-python lazily so fakes can be injected without it installed."""
    from kafka import KafkaProducer  # noqa: PLC0415
    return KafkaProducer(**kwargs)


class Producer:
    """Publishes payloads to the topic matching their DocRequestCode."""

    def __init__(
        self,
        client: Any,
        print_payload: bool = False,
    ) -> None:
        self.client = client
        self.print_payload = print_payload
        self.published = 0

    # --- publishing ----------------------------------------------------

    def publish(self, payload: dict[str, Any]) -> tuple[str, int, int]:
        """Send one payload to its topic. Returns (topic, partition, offset).

        The envelope mirrors the Redis Streams version's flat field map so the
        two backends carry the same routing keys, just JSON-encoded as the
        Kafka record value instead of a RESP field/value map.
        """
        if self.print_payload:
            print(json.dumps(payload, indent=4, ensure_ascii=False))

        topic = topic_for_payload(payload)
        main = payload["Fields"]["Main"]
        envelope = {
            "payload": payload,
            "request_id": payload["RequestId"],
            "doc_request_code": payload["DocRequestCode"],
            "application_no": main["ApplicationNo"],
            "agreement_no": main["AgreementNo"],
            "produced_at_ms": int(time.time() * 1000),
            "schema_version": "1",
        }
        value = json.dumps(envelope, separators=(",", ":"), ensure_ascii=False)
        future = self.client.send(topic, value=value, key=payload["RequestId"])
        record_metadata = future.get(timeout=10)
        self.published += 1
        return record_metadata.topic, record_metadata.partition, record_metadata.offset

    # --- main loop -------------------------------------------------------

    def run(self, rate: float, count: int,
            on_publish: Callable[[dict[str, Any], str, int, int], None] | None = None) -> None:
        interval = 1.0 / rate if rate > 0 else 0.0
        log.info("producing to %r at %.3g msg/s (target=%s)",
                 config.KAFKA_BOOTSTRAP_SERVERS, rate, count or "unbounded")

        seq = 0
        next_tick = time.monotonic()
        while _running:
            seq += 1
            payload = generate_payload(seq)
            try:
                topic, partition, offset = self.publish(payload)
            except Exception as exc:  # noqa: BLE001 - surfaced to caller/log
                log.error("send failed for %s: %s", payload["RequestId"], exc)
                continue

            if on_publish is not None:
                on_publish(payload, topic, partition, offset)

            log.info("SEND %s p%d@%d app=%s doc=%s amount=%s term=%sm",
                     topic, partition, offset,
                     payload["Fields"]["Main"]["ApplicationNo"],
                     payload["DocRequestCode"],
                     payload["Fields"]["Finance"]["FinanceAmount"],
                     payload["Fields"]["Finance"]["Months"])

            if count and self.published >= count:
                log.info("reached target of %d messages", count)
                break

            next_tick += interval
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                next_tick = time.monotonic()

        try:
            self.client.flush(timeout=10)
        except Exception:  # noqa: BLE001
            pass
        log.info("stopped after publishing %d messages", self.published)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Produce randomised application payloads to Kafka topics.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--bootstrap-servers", default=config.KAFKA_BOOTSTRAP_SERVERS,
                   help="comma separated host:port list")
    p.add_argument("--client-id", default=config.KAFKA_CLIENT_ID)
    p.add_argument("--rate", type=float, default=config.KAFKA_PRODUCE_RATE,
                   help="messages per second")
    p.add_argument("--count", type=int, default=config.KAFKA_PRODUCE_COUNT,
                   help="stop after N messages (0 = run forever)")
    p.add_argument("--acks", default=config.KAFKA_ACKS)
    p.add_argument("--max-block-ms", type=int, default=config.KAFKA_MAX_BLOCK_MS,
                   help="how long send() blocks on metadata before raising "
                        "KafkaTimeoutError")
    p.add_argument("--api-version", default=config.KAFKA_API_VERSION,
                   help="pin the wire protocol version, e.g. 0.10.1 -- try this "
                        "if metadata calls work but send() times out against a "
                        "broker that only implements part of the Kafka protocol")
    p.add_argument("--print-payload", action="store_true",
                   help="pretty print each generated payload")
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

    kwargs = config.kafka_producer_kwargs(args.bootstrap_servers, args.api_version)
    kwargs.update(client_id=args.client_id, acks=args.acks, max_block_ms=args.max_block_ms)
    try:
        client = _build_client(**kwargs)
    except ImportError:
        log.error("kafka-python is not installed -- run: pip install kafka-python")
        return 1
    except Exception as exc:  # noqa: BLE001
        log.error("cannot reach Kafka at %s -- %s", args.bootstrap_servers, exc)
        return 1
    log.info("connected to Kafka at %s", args.bootstrap_servers)

    if not args.no_ensure_topics:
        ok, message = ensure_topics_exist(args.bootstrap_servers,
                                          config.parse_api_version(args.api_version))
        (log.info if ok else log.warning)("ensure topics: %s", message)
        if not ok:
            log.warning("if the broker does not auto-create topics, send() may "
                       "hang until KafkaTimeoutError -- create them manually "
                       "(see README) if this keeps happening")

    producer = Producer(client, print_payload=args.print_payload)
    try:
        producer.run(args.rate, args.count)
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
