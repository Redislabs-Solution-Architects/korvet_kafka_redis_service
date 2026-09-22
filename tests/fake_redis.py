"""Minimal in-memory stand-in for the redis-py stream API.

Only exists so the producer and consumer can be exercised without a Redis
server. It mirrors the redis-py method names, keyword arguments and return
shapes for the commands this project uses, and nothing else. Not a general
purpose fake, and no substitute for running against real Redis.
"""

from __future__ import annotations

import time
from typing import Any


class RedisError(Exception):
    pass


class ResponseError(RedisError):
    pass


class ConnectionError(RedisError):  # noqa: A001 - matches redis.exceptions
    pass


class DataError(RedisError):
    pass


def _id_tuple(entry_id: str) -> tuple[int, int]:
    ms, _, seq = entry_id.partition("-")
    return int(ms), int(seq or 0)


class _Group:
    def __init__(self, last_delivered: str) -> None:
        self.last_delivered = last_delivered
        # entry_id -> {"consumer": name, "delivered_at_ms": int, "count": int}
        self.pending: dict[str, dict[str, Any]] = {}


class FakeRedis:
    def __init__(self, **_kwargs: Any) -> None:
        # stream key -> list of (id, fields) kept in ID order
        self.streams: dict[str, list[tuple[str, dict[str, str]]]] = {}
        self.groups: dict[str, dict[str, _Group]] = {}
        self._last_ms = 0
        self._seq = 0
        self.closed = False

    # --- plumbing ----------------------------------------------------------

    def ping(self) -> bool:
        return True

    def close(self) -> None:
        self.closed = True

    def _next_id(self) -> str:
        ms = int(time.time() * 1000)
        if ms == self._last_ms:
            self._seq += 1
        else:
            self._last_ms, self._seq = ms, 0
        return f"{ms}-{self._seq}"

    # --- writes ------------------------------------------------------------

    def xadd(self, name: str, fields: dict[str, Any], id: str = "*",  # noqa: A002
             **_kwargs: Any) -> str:
        entry_id = self._next_id() if id == "*" else id
        self.streams.setdefault(name, []).append(
            (entry_id, {k: str(v) for k, v in fields.items()}))
        return entry_id

    def xlen(self, name: str) -> int:
        return len(self.streams.get(name, []))

    def xrange(self, name: str, min: str = "-", max: str = "+",  # noqa: A002
               count: int | None = None) -> list[tuple[str, dict[str, str]]]:
        entries = self.streams.get(name, [])
        return entries[:count] if count else list(entries)

    def xtrim(self, name: str, maxlen: int | None = None, approximate: bool = True,
              minid: int | str | None = None, limit: int | None = None) -> int:
        if maxlen is not None and minid is not None:
            raise DataError("Only one of ``maxlen`` or ``minid`` may be specified")
        if maxlen is None and minid is None:
            raise DataError("One of ``maxlen`` or ``minid`` must be specified")

        entries = self.streams.get(name)
        if not entries:
            return 0
        before = len(entries)

        if minid is not None:
            floor = _id_tuple(str(minid))
            self.streams[name] = [e for e in entries if _id_tuple(e[0]) >= floor]
        else:
            assert maxlen is not None
            if before > maxlen:
                self.streams[name] = entries[before - maxlen:]
        return before - len(self.streams[name])

    # --- consumer groups ---------------------------------------------------

    def xgroup_create(self, name: str, groupname: str, id: str = "$",  # noqa: A002
                      mkstream: bool = False, **_kwargs: Any) -> bool:
        if name not in self.streams:
            if not mkstream:
                raise ResponseError("NOGROUP No such key")
            self.streams[name] = []
        groups = self.groups.setdefault(name, {})
        if groupname in groups:
            raise ResponseError(
                "BUSYGROUP Consumer Group name already exists")
        entries = self.streams[name]
        last = entries[-1][0] if (id == "$" and entries) else ("0-0" if id == "0" else id)
        groups[groupname] = _Group(last)
        return True

    def _group(self, name: str, groupname: str) -> _Group:
        try:
            return self.groups[name][groupname]
        except KeyError:
            raise ResponseError(
                f"NOGROUP No such consumer group '{groupname}' for key name '{name}'"
            ) from None

    def xreadgroup(self, groupname: str, consumername: str, streams: dict[str, str],
                   count: int | None = None, block: int | None = None,
                   noack: bool = False) -> list:
        out = []
        for name, cursor in streams.items():
            group = self._group(name, groupname)
            if cursor != ">":
                raise NotImplementedError("fake only supports the '>' cursor")
            floor = _id_tuple(group.last_delivered)
            fresh = [e for e in self.streams.get(name, []) if _id_tuple(e[0]) > floor]
            if count:
                fresh = fresh[:count]
            if not fresh:
                continue
            now_ms = int(time.time() * 1000)
            for entry_id, _fields in fresh:
                group.last_delivered = entry_id
                if not noack:
                    group.pending[entry_id] = {
                        "consumer": consumername,
                        "delivered_at_ms": now_ms,
                        "count": 1,
                    }
            out.append([name, fresh])
        return out

    def xack(self, name: str, groupname: str, *ids: str) -> int:
        group = self._group(name, groupname)
        return sum(1 for i in ids if group.pending.pop(i, None) is not None)

    def xpending(self, name: str, groupname: str) -> dict[str, Any]:
        group = self._group(name, groupname)
        ids = sorted(group.pending, key=_id_tuple)
        return {
            "pending": len(ids),
            "min": ids[0] if ids else None,
            "max": ids[-1] if ids else None,
            "consumers": [],
        }

    def xautoclaim(self, name: str, groupname: str, consumername: str,
                   min_idle_time: int, start_id: str = "0-0",
                   count: int | None = None, justid: bool = False) -> list:
        group = self._group(name, groupname)
        by_id = dict(self.streams.get(name, []))
        now_ms = int(time.time() * 1000)
        floor = _id_tuple(start_id)

        claimed: list[tuple[str, dict[str, str] | None]] = []
        deleted: list[str] = []
        for entry_id in sorted(group.pending, key=_id_tuple):
            if _id_tuple(entry_id) < floor:
                continue
            info = group.pending[entry_id]
            if (now_ms - info["delivered_at_ms"]) < min_idle_time:
                continue
            if entry_id not in by_id:
                # Entry was trimmed away; Redis 7 drops it from the PEL.
                group.pending.pop(entry_id, None)
                deleted.append(entry_id)
                continue
            info["consumer"] = consumername
            info["delivered_at_ms"] = now_ms
            info["count"] += 1
            claimed.append((entry_id, by_id[entry_id]))
            if count and len(claimed) >= count:
                break
        return ["0-0", claimed, deleted]
