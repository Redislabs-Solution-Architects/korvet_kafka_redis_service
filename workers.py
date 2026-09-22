"""Start and stop producer/consumer worker threads, instrumented into Metrics.

Shared by both dashboards so they behave identically. Standard library only.

Two things this module exists to get right:

1. **One Redis client per thread.** `redis_client.Redis` wraps a single socket and
   is explicitly not thread safe. Sharing one across the producer and consumer
   threads would interleave writes and desynchronise the RESP reader, which
   surfaces as baffling parse errors under load. Every worker builds its own.

2. **Per-worker stop flags.** `producer.py` and `consumer.py` use a module level
   `_running` global for Ctrl-C handling, which cannot express "stop the producer
   but leave the consumer running". Workers here loop on their own
   `threading.Event`, and reuse the `Producer`/`Consumer` classes for the actual
   Redis work so there is one implementation of publish, trim, read and ack.
"""

from __future__ import annotations

import logging
import threading
import time
from typing import Any, Callable

import config
import redis_client
from consumer import Consumer
from metrics import Metrics
from payloads import generate_payload
from producer import Producer
from redis_client import RedisError, ResponseError

log = logging.getLogger("workers")


class _InstrumentedConsumer(Consumer):
    """Consumer that reports each handled entry to a Metrics instance."""

    def __init__(self, *args: Any, metrics: Metrics, **kwargs: Any) -> None:
        super().__init__(*args, **kwargs)
        self._metrics = metrics

    def handle(self, entry_id: str, fields: dict[str, str]) -> bool:
        produced_at = fields.get("produced_at_ms")
        lag_ms: float | None = None
        if produced_at:
            try:
                lag_ms = max(0.0, time.time() * 1000 - float(produced_at))
            except ValueError:
                lag_ms = None
        result = super().handle(entry_id, fields)
        self._metrics.record_consumed(lag_ms)
        if result:
            self._metrics.record_acked()
        else:
            self._metrics.record_failed()
        return result


class WorkerManager:
    """Owns the producer and consumer threads and the shared Metrics instance.

    Safe to call start/stop from a UI thread. Each start is idempotent: calling
    it while already running is a no-op rather than a second worker.
    """

    def __init__(self, metrics: Metrics | None = None) -> None:
        self.metrics = metrics or Metrics()
        self._lock = threading.Lock()

        self._producer_thread: threading.Thread | None = None
        self._producer_stop = threading.Event()
        self._consumer_thread: threading.Thread | None = None
        self._consumer_stop = threading.Event()

        # Connection settings, mutable from the UI while stopped.
        self.settings: dict[str, Any] = {
            "host": config.REDIS_HOST,
            "port": config.REDIS_PORT,
            "db": config.REDIS_DB,
            "username": config.REDIS_USERNAME,
            "password": config.REDIS_PASSWORD,
            "ssl": config.REDIS_SSL,
            "ssl_verify": config.REDIS_SSL_VERIFY,
            "stream": config.STREAM_KEY,
            "group": config.CONSUMER_GROUP,
            "ttl_seconds": config.TTL_SECONDS,
            "max_len": config.MAX_STREAM_LEN,
            "trim_interval": config.TRIM_INTERVAL_SECONDS,
            "rate": config.PRODUCE_RATE,
            "batch_size": config.BATCH_SIZE,
            "block_ms": config.BLOCK_MS,
            "claim_min_idle_ms": config.CLAIM_MIN_IDLE_MS,
            "claim_interval": config.CLAIM_INTERVAL_SECONDS,
            "consumer_name": "dashboard-1",
        }

        # Test seam: swapped for a fake in the test suite.
        self._client_factory: Callable[..., Any] = redis_client.Redis

    # --- connection --------------------------------------------------------

    def _client_kwargs(self) -> dict[str, Any]:
        s = self.settings
        kwargs = config.redis_kwargs()
        kwargs.update(host=s["host"], port=s["port"], db=s["db"])
        if s.get("username"):
            kwargs["username"] = s["username"]
        if s.get("password"):
            kwargs["password"] = s["password"]
        if s.get("ssl"):
            kwargs["ssl"] = True
            kwargs["ssl_cert_reqs"] = "required" if s.get("ssl_verify") else "none"
        return kwargs

    def new_client(self) -> Any:
        """A fresh client. Never share the result between threads."""
        return self._client_factory(**self._client_kwargs())

    def ping(self) -> tuple[bool, str]:
        """Check the endpoint from the UI thread. Returns (ok, message)."""
        client = None
        try:
            client = self.new_client()
            client.ping()
            version = "unknown"
            try:
                version = client.info("server").get("redis_version", "unknown")
            except RedisError:
                pass
            return True, f"connected, Redis {version}"
        except Exception as exc:  # noqa: BLE001 - surfaced verbatim in the UI
            return False, f"{type(exc).__name__}: {exc}"
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:  # noqa: BLE001
                    pass

    def stream_status(self) -> dict[str, Any]:
        """Length and pending count, read on demand from the UI thread."""
        client = None
        out: dict[str, Any] = {"length": None, "pending": None,
                               "consumers": [], "error": None}
        try:
            client = self.new_client()
            stream, group = self.settings["stream"], self.settings["group"]
            if not client.exists(stream):
                out["length"] = 0
                return out
            out["length"] = client.xlen(stream)
            try:
                pending = client.xpending(stream, group)
                out["pending"] = pending["pending"]
                out["consumers"] = pending["consumers"]
            except ResponseError:
                out["pending"] = None      # group does not exist yet
        except Exception as exc:  # noqa: BLE001
            out["error"] = f"{type(exc).__name__}: {exc}"
        finally:
            if client is not None:
                try:
                    client.close()
                except Exception:  # noqa: BLE001
                    pass
        return out

    # --- producer ----------------------------------------------------------

    def producer_running(self) -> bool:
        t = self._producer_thread
        return t is not None and t.is_alive()

    def start_producer(self) -> bool:
        with self._lock:
            if self.producer_running():
                return False
            self._producer_stop.clear()
            self._producer_thread = threading.Thread(
                target=self._producer_loop, name="producer", daemon=True)
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
            client = self.new_client()
            prod = Producer(client, s["stream"], s["ttl_seconds"],
                            s["max_len"], s["trim_interval"])
            rate = float(s["rate"])
            interval = 1.0 / rate if rate > 0 else 0.0
            seq = 0
            next_tick = time.monotonic()

            while not self._producer_stop.is_set():
                seq += 1
                try:
                    prod.publish(generate_payload(seq))
                    self.metrics.record_produced()
                except redis_client.ConnectionError as exc:
                    self.metrics.record_error(f"producer connection lost: {exc}")
                    # Rebuild the client; the old socket is unusable.
                    if self._producer_stop.wait(2.0):
                        break
                    try:
                        client.close()
                    except Exception:  # noqa: BLE001
                        pass
                    client = self.new_client()
                    prod.client = client
                    continue
                except RedisError as exc:
                    self.metrics.record_error(f"producer XADD failed: {exc}")
                    if self._producer_stop.wait(0.5):
                        break
                    continue

                try:
                    trimmed = prod.trim()
                    if trimmed:
                        self.metrics.record_trimmed(trimmed)
                except RedisError as exc:
                    self.metrics.record_error(f"producer trim failed: {exc}")

                next_tick += interval
                sleep_for = next_tick - time.monotonic()
                if sleep_for > 0:
                    # Event.wait doubles as an interruptible sleep, so stopping
                    # is immediate rather than waiting out the tick.
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
                    client.close()
                except Exception:  # noqa: BLE001
                    pass
            self.metrics.mark_producer_stopped()

    # --- consumer ----------------------------------------------------------

    def consumer_running(self) -> bool:
        t = self._consumer_thread
        return t is not None and t.is_alive()

    def start_consumer(self) -> bool:
        with self._lock:
            if self.consumer_running():
                return False
            self._consumer_stop.clear()
            self._consumer_thread = threading.Thread(
                target=self._consumer_loop, name="consumer", daemon=True)
            self._consumer_thread.start()
            self.metrics.mark_consumer_started()
            return True

    def stop_consumer(self, timeout: float = 10.0) -> bool:
        self._consumer_stop.set()
        t = self._consumer_thread
        if t is not None and t.is_alive():
            # Worst case this waits out one BLOCK interval.
            t.join(timeout=timeout)
        self.metrics.mark_consumer_stopped()
        return not (t is not None and t.is_alive())

    def _consumer_loop(self) -> None:
        s = self.settings
        client = None
        try:
            client = self.new_client()
            cons = _InstrumentedConsumer(
                client, s["stream"], s["group"], s["consumer_name"],
                s["batch_size"], s["block_ms"],
                s["claim_min_idle_ms"], s["claim_interval"],
                metrics=self.metrics,
            )
            cons.ensure_group()

            while not self._consumer_stop.is_set():
                try:
                    before = cons.processed
                    cons.reclaim_stale()
                    if cons.processed > before:
                        self.metrics.record_reclaimed(cons.processed - before)

                    response = client.xreadgroup(
                        groupname=s["group"],
                        consumername=s["consumer_name"],
                        streams={s["stream"]: ">"},
                        count=s["batch_size"],
                        block=s["block_ms"],
                    )
                except redis_client.ConnectionError as exc:
                    self.metrics.record_error(f"consumer connection lost: {exc}")
                    if self._consumer_stop.wait(2.0):
                        break
                    try:
                        client.close()
                    except Exception:  # noqa: BLE001
                        pass
                    client = self.new_client()
                    cons.client = client
                    continue
                except ResponseError as exc:
                    if "NOGROUP" in str(exc):
                        self.metrics.record_error(f"group vanished, recreating: {exc}")
                        cons.ensure_group()
                        continue
                    self.metrics.record_error(f"consumer XREADGROUP failed: {exc}")
                    if self._consumer_stop.wait(1.0):
                        break
                    continue
                except RedisError as exc:
                    self.metrics.record_error(f"consumer read failed: {exc}")
                    if self._consumer_stop.wait(1.0):
                        break
                    continue

                if not response:
                    continue
                for _stream_name, entries in response:
                    cons._process_batch(entries, "dashboard")
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

    # --- combined ----------------------------------------------------------

    def stop_all(self) -> None:
        self.stop_producer()
        self.stop_consumer()

    def status(self) -> dict[str, Any]:
        return {
            "producer_running": self.producer_running(),
            "consumer_running": self.consumer_running(),
            "settings": {k: ("***" if k == "password" and v else v)
                         for k, v in self.settings.items()},
        }
