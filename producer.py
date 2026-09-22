#!/usr/bin/env python3
"""Redis Streams producer for randomised BigCreditBank application payloads.

Generates a fresh payload on every tick, XADDs it to the stream, and enforces a
300 second retention window by trimming entries whose ID timestamp has aged out.

Run with --help for the full flag list.
"""

from __future__ import annotations

import argparse
import json
import logging
import signal
import sys
import time
from typing import Any

import config
import redis_client as redis
from payloads import generate_payload
from redis_client import ConnectionError as RedisConnectionError, RedisError

log = logging.getLogger("producer")

_running = True


def _handle_signal(signum: int, _frame: Any) -> None:
    global _running
    log.info("received %s, finishing current tick then stopping",
             signal.Signals(signum).name)
    _running = False


def _install_signal_handlers() -> None:
    """Register handlers, unless we are embedded in a non-main thread."""
    for sig in (signal.SIGINT, signal.SIGTERM):
        try:
            signal.signal(sig, _handle_signal)
        except ValueError:
            log.debug("not the main thread, skipping %s handler", sig.name)


class Producer:
    """Publishes payloads to a stream and keeps it inside the retention window."""

    def __init__(
        self,
        client: redis.Redis,
        stream: str,
        ttl_seconds: int,
        max_len: int,
        trim_interval: float,
        print_payload: bool = False,
    ) -> None:
        self.client = client
        self.stream = stream
        self.ttl_seconds = ttl_seconds
        self.max_len = max_len
        self.trim_interval = trim_interval
        self.print_payload = print_payload
        self._last_trim = 0.0
        self.published = 0

    # --- publishing --------------------------------------------------------

    def publish(self, payload: dict[str, Any]) -> str:
        """XADD one payload. Returns the assigned stream entry ID.

        The envelope is flat because stream entries are a flat field/value map:
        the application document goes into `payload` as compact JSON and the
        useful routing keys are lifted out alongside it so a consumer can filter
        or log without parsing the whole body.
        """
        if self.print_payload:
            print(json.dumps(payload, indent=4, ensure_ascii=False))

        main = payload["Fields"]["Main"]
        entry = {
            "payload": json.dumps(payload, separators=(",", ":"), ensure_ascii=False),
            "request_id": payload["RequestId"],
            "doc_request_code": payload["DocRequestCode"],
            "application_no": main["ApplicationNo"],
            "agreement_no": main["AgreementNo"],
            "produced_at_ms": str(int(time.time() * 1000)),
            "schema_version": "1",
        }
        entry_id = self.client.xadd(self.stream, entry, id="*")
        self.published += 1
        return entry_id

    # --- retention ---------------------------------------------------------

    def trim(self, force: bool = False) -> int:
        """Drop entries older than the TTL window.

        Redis Streams have no per-message expiry. Stream IDs are
        `<unix-millis>-<seq>`, so `XTRIM ... MINID <now - ttl>` removes exactly
        the entries whose logical age exceeds the TTL. Unlike EXPIRE on the key
        this never destroys the stream itself or its consumer groups, so a
        consumer that reconnects still finds its group intact.

        MAXLEN is applied as a separate safety net so a stalled consumer cannot
        let the stream grow without bound inside the window.
        """
        now = time.monotonic()
        if not force and (now - self._last_trim) < self.trim_interval:
            return 0
        self._last_trim = now

        min_id = int((time.time() - self.ttl_seconds) * 1000)
        removed = 0
        try:
            # approximate=False so the trim is exact; these streams are small
            # enough that the radix-node shortcut is not worth the imprecision.
            removed += self.client.xtrim(self.stream, minid=min_id, approximate=False)
            if self.max_len > 0:
                removed += self.client.xtrim(self.stream, maxlen=self.max_len,
                                             approximate=True)
        except RedisError as exc:
            log.warning("trim failed: %s", exc)
            return 0

        if removed:
            log.info("trimmed %d entr%s older than %ds",
                     removed, "y" if removed == 1 else "ies", self.ttl_seconds)
        return removed

    # --- main loop ---------------------------------------------------------

    def run(self, rate: float, count: int) -> None:
        interval = 1.0 / rate if rate > 0 else 0.0
        log.info("producing to %r at %.3g msg/s (ttl=%ds, target=%s)",
                 self.stream, rate, self.ttl_seconds, count or "unbounded")

        seq = 0
        next_tick = time.monotonic()
        while _running:
            seq += 1
            payload = generate_payload(seq)
            try:
                entry_id = self.publish(payload)
            except RedisConnectionError as exc:
                log.error("connection lost, retrying in 2s: %s", exc)
                time.sleep(2)
                seq -= 1
                continue
            except RedisError as exc:
                log.error("XADD failed for %s: %s", payload["RequestId"], exc)
                continue

            log.info("XADD %s id=%s app=%s amount=%s term=%sm",
                     self.stream, entry_id,
                     payload["Fields"]["Main"]["ApplicationNo"],
                     payload["Fields"]["Finance"]["FinanceAmount"],
                     payload["Fields"]["Finance"]["Months"])

            self.trim()

            if count and self.published >= count:
                log.info("reached target of %d messages", count)
                break

            next_tick += interval
            sleep_for = next_tick - time.monotonic()
            if sleep_for > 0:
                time.sleep(sleep_for)
            else:
                # Fell behind schedule; reset rather than accumulate debt.
                next_tick = time.monotonic()

        self.trim(force=True)
        log.info("stopped after publishing %d messages", self.published)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    p = argparse.ArgumentParser(
        description="Produce randomised application payloads to a Redis stream.",
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
    p.add_argument("--rate", type=float, default=config.PRODUCE_RATE,
                   help="messages per second")
    p.add_argument("--count", type=int, default=config.PRODUCE_COUNT,
                   help="stop after N messages (0 = run forever)")
    p.add_argument("--ttl", type=int, default=config.TTL_SECONDS,
                   help="retention window in seconds, enforced with XTRIM MINID")
    p.add_argument("--max-len", type=int, default=config.MAX_STREAM_LEN,
                   help="MAXLEN safety cap (0 = disabled)")
    p.add_argument("--trim-interval", type=float, default=config.TRIM_INTERVAL_SECONDS,
                   help="seconds between trim passes")
    p.add_argument("--print-payload", action="store_true",
                   help="pretty print each generated payload")
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

    producer = Producer(client, args.stream, args.ttl, args.max_len,
                        args.trim_interval, print_payload=args.print_payload)
    try:
        producer.run(args.rate, args.count)
    finally:
        client.close()
    return 0


if __name__ == "__main__":
    sys.exit(main())
