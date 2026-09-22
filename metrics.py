"""Thread-safe throughput and latency metrics, standard library only.

Shared by both dashboards (`streamlit_app.py` and `dashboard.py`) so the numbers
they show come from exactly the same place.

Design notes worth knowing:

* Counters are bucketed by whole epoch second. `series()` deliberately **excludes
  the current, still-filling second**, otherwise every chart shows a fake dip at
  the right hand edge.
* Latency samples are kept per second bucket so percentiles can be charted over
  time, and separately in a bounded recent window for the summary cards.
* Everything is guarded by one lock. The critical sections are a few dict
  updates, so contention is irrelevant next to the Redis round trip.
* Memory is bounded by `window_seconds`: old buckets are pruned on every write.
"""

from __future__ import annotations

import threading
import time
from typing import Any

# Cap the per-second latency sample list so a very high rate cannot balloon
# memory. Percentiles over a few hundred samples per second are plenty accurate.
MAX_SAMPLES_PER_BUCKET = 500


def percentile(sorted_values: list[float], pct: float) -> float:
    """Nearest-rank percentile. `sorted_values` must already be sorted."""
    if not sorted_values:
        return 0.0
    if len(sorted_values) == 1:
        return sorted_values[0]
    k = (len(sorted_values) - 1) * (pct / 100.0)
    lo = int(k)
    hi = min(lo + 1, len(sorted_values) - 1)
    if lo == hi:
        return sorted_values[lo]
    # Linear interpolation between the two straddling ranks.
    return sorted_values[lo] + (sorted_values[hi] - sorted_values[lo]) * (k - lo)


class _Bucket:
    __slots__ = ("produced", "consumed", "acked", "failed", "trimmed",
                 "errors", "reclaimed", "lags", "_dropped")

    def __init__(self) -> None:
        self.produced = 0
        self.consumed = 0
        self.acked = 0
        self.failed = 0
        self.trimmed = 0
        self.errors = 0
        self.reclaimed = 0
        self.lags: list[float] = []
        self._dropped = 0

    def add_lag(self, lag_ms: float) -> None:
        if len(self.lags) < MAX_SAMPLES_PER_BUCKET:
            self.lags.append(lag_ms)
        else:
            self._dropped += 1


class Metrics:
    """Collects counters and latencies, and renders JSON-friendly snapshots."""

    def __init__(self, window_seconds: int = 300) -> None:
        self.window_seconds = window_seconds
        self._lock = threading.Lock()
        self._buckets: dict[int, _Bucket] = {}
        self._totals: dict[str, int] = {
            "produced": 0, "consumed": 0, "acked": 0, "failed": 0,
            "trimmed": 0, "errors": 0, "reclaimed": 0,
        }
        self._recent_lags: list[float] = []       # bounded, for the summary cards
        self._recent_lags_cap = 2000
        self._started_at = time.time()
        self._producer_started_at: float | None = None
        self._consumer_started_at: float | None = None
        self._last_error: str | None = None

    # --- recording ---------------------------------------------------------

    def _bucket(self, now: float | None = None) -> _Bucket:
        """Caller must hold the lock."""
        sec = int(now if now is not None else time.time())
        bucket = self._buckets.get(sec)
        if bucket is None:
            bucket = self._buckets[sec] = _Bucket()
            # Prune while we are here rather than on a timer. The cutoff is
            # measured from the real clock, not from `sec`: keying it off the
            # bucket being written would let a stale timestamp (a backdated
            # write, or a clock step) leave old buckets in place forever.
            cutoff = int(time.time()) - self.window_seconds
            for old in [s for s in self._buckets if s < cutoff]:
                del self._buckets[old]
        return bucket

    def record_produced(self, n: int = 1) -> None:
        with self._lock:
            self._bucket().produced += n
            self._totals["produced"] += n

    def record_consumed(self, lag_ms: float | None = None, n: int = 1) -> None:
        with self._lock:
            b = self._bucket()
            b.consumed += n
            self._totals["consumed"] += n
            if lag_ms is not None and lag_ms >= 0:
                b.add_lag(lag_ms)
                self._recent_lags.append(lag_ms)
                if len(self._recent_lags) > self._recent_lags_cap:
                    # Drop the oldest half in one slice; cheaper than popping.
                    del self._recent_lags[: self._recent_lags_cap // 2]

    def record_acked(self, n: int = 1) -> None:
        with self._lock:
            self._bucket().acked += n
            self._totals["acked"] += n

    def record_failed(self, n: int = 1) -> None:
        with self._lock:
            self._bucket().failed += n
            self._totals["failed"] += n

    def record_trimmed(self, n: int = 1) -> None:
        with self._lock:
            self._bucket().trimmed += n
            self._totals["trimmed"] += n

    def record_reclaimed(self, n: int = 1) -> None:
        with self._lock:
            self._bucket().reclaimed += n
            self._totals["reclaimed"] += n

    def record_error(self, message: str) -> None:
        with self._lock:
            self._bucket().errors += 1
            self._totals["errors"] += 1
            self._last_error = message

    # --- lifecycle markers -------------------------------------------------

    def mark_producer_started(self) -> None:
        with self._lock:
            self._producer_started_at = time.time()

    def mark_producer_stopped(self) -> None:
        with self._lock:
            self._producer_started_at = None

    def mark_consumer_started(self) -> None:
        with self._lock:
            self._consumer_started_at = time.time()

    def mark_consumer_stopped(self) -> None:
        with self._lock:
            self._consumer_started_at = None

    def reset(self) -> None:
        with self._lock:
            self._buckets.clear()
            for k in self._totals:
                self._totals[k] = 0
            self._recent_lags.clear()
            self._last_error = None
            self._started_at = time.time()

    # --- reading -----------------------------------------------------------

    def series(self, window_seconds: int | None = None) -> list[dict[str, Any]]:
        """Per-second time series, oldest first.

        The current second is excluded because it is still filling and would
        render as a spurious drop at the right edge of every chart.
        """
        window = window_seconds or self.window_seconds
        with self._lock:
            now_sec = int(time.time())
            oldest = now_sec - window
            rows = []
            # Fill gaps with zeros so the chart shows idle periods honestly
            # rather than joining across them with a straight line.
            for sec in range(max(oldest, now_sec - self.window_seconds), now_sec):
                b = self._buckets.get(sec)
                if b is None:
                    rows.append({
                        "t": sec, "produced": 0, "consumed": 0, "acked": 0,
                        "failed": 0, "trimmed": 0, "reclaimed": 0, "errors": 0,
                        "lag_p50": None, "lag_p95": None, "lag_max": None,
                        "samples": 0,
                    })
                    continue
                lags = sorted(b.lags)
                rows.append({
                    "t": sec,
                    "produced": b.produced,
                    "consumed": b.consumed,
                    "acked": b.acked,
                    "failed": b.failed,
                    "trimmed": b.trimmed,
                    "reclaimed": b.reclaimed,
                    "errors": b.errors,
                    "lag_p50": round(percentile(lags, 50), 2) if lags else None,
                    "lag_p95": round(percentile(lags, 95), 2) if lags else None,
                    "lag_max": round(max(lags), 2) if lags else None,
                    "samples": len(lags),
                })
            return rows

    def rates(self, over_seconds: int = 10) -> dict[str, float]:
        """Average messages per second over the last N complete seconds."""
        with self._lock:
            now_sec = int(time.time())
            span = range(now_sec - over_seconds, now_sec)
            produced = sum(self._buckets[s].produced for s in span if s in self._buckets)
            consumed = sum(self._buckets[s].consumed for s in span if s in self._buckets)
            acked = sum(self._buckets[s].acked for s in span if s in self._buckets)
            n = max(over_seconds, 1)
            return {
                "produce_per_s": round(produced / n, 2),
                "consume_per_s": round(consumed / n, 2),
                "ack_per_s": round(acked / n, 2),
                "over_seconds": over_seconds,
            }

    def latency(self) -> dict[str, float | int | None]:
        """Percentiles over the recent bounded sample window."""
        with self._lock:
            lags = sorted(self._recent_lags)
        if not lags:
            return {"p50": None, "p95": None, "p99": None, "max": None,
                    "min": None, "mean": None, "samples": 0}
        return {
            "p50": round(percentile(lags, 50), 2),
            "p95": round(percentile(lags, 95), 2),
            "p99": round(percentile(lags, 99), 2),
            "max": round(lags[-1], 2),
            "min": round(lags[0], 2),
            "mean": round(sum(lags) / len(lags), 2),
            "samples": len(lags),
        }

    def totals(self) -> dict[str, int]:
        with self._lock:
            return dict(self._totals)

    def snapshot(self, window_seconds: int = 120,
                 rate_over: int = 10) -> dict[str, Any]:
        """Everything a dashboard needs, in one JSON-serialisable dict."""
        with self._lock:
            uptime = time.time() - self._started_at
            prod_started = self._producer_started_at
            cons_started = self._consumer_started_at
            last_error = self._last_error
        now = time.time()
        return {
            "now": now,
            "uptime_seconds": round(uptime, 1),
            "totals": self.totals(),
            "rates": self.rates(rate_over),
            "latency_ms": self.latency(),
            "series": self.series(window_seconds),
            "producer_uptime_seconds": (round(now - prod_started, 1)
                                        if prod_started else None),
            "consumer_uptime_seconds": (round(now - cons_started, 1)
                                        if cons_started else None),
            "last_error": last_error,
        }
