"""A real TCP server speaking RESP, backed by the in-memory stream logic.

Used to test redis_client.py on the wire: actual sockets, actual RESP framing,
actual command encoding. It records every command it receives so tests can
assert on the exact bytes the client produced, which is where protocol bugs
hide (wrong argument order, missing MINID marker, BLOCK omitted).

It deliberately writes replies in small fragments to exercise the client's
incremental reader.
"""

from __future__ import annotations

import socket
import threading
from typing import Any

import fake_redis

CRLF = b"\r\n"


# --- RESP serialisation ---------------------------------------------------

def encode(value: Any) -> bytes:
    if value is None:
        return b"$-1\r\n"
    if value is True:
        return b":1\r\n"
    if value is False:
        return b":0\r\n"
    if isinstance(value, int):
        return b":%d\r\n" % value
    if isinstance(value, _Simple):
        return b"+" + value.text.encode() + CRLF
    if isinstance(value, _Error):
        return b"-" + value.text.encode() + CRLF
    if isinstance(value, (str, bytes)):
        raw = value.encode() if isinstance(value, str) else value
        return b"$%d\r\n%s\r\n" % (len(raw), raw)
    if isinstance(value, (list, tuple)):
        return b"*%d\r\n" % len(value) + b"".join(encode(v) for v in value)
    raise TypeError(f"cannot encode {type(value)}")


class _Simple:
    def __init__(self, text: str) -> None:
        self.text = text


class _Error:
    def __init__(self, text: str) -> None:
        self.text = text


OK = _Simple("OK")


# --- RESP parsing (client -> server) --------------------------------------

class _CommandReader:
    def __init__(self, sock: socket.socket) -> None:
        self._sock = sock
        self._buf = bytearray()

    def _fill(self) -> bool:
        chunk = self._sock.recv(65536)
        if not chunk:
            return False
        self._buf.extend(chunk)
        return True

    def _line(self) -> bytes | None:
        while True:
            i = self._buf.find(CRLF)
            if i >= 0:
                line = bytes(self._buf[:i])
                del self._buf[: i + 2]
                return line
            if not self._fill():
                return None

    def _bulk(self, n: int) -> bytes | None:
        while len(self._buf) < n + 2:
            if not self._fill():
                return None
        data = bytes(self._buf[:n])
        del self._buf[: n + 2]
        return data

    def read_command(self) -> list[str] | None:
        line = self._line()
        if line is None:
            return None
        if not line.startswith(b"*"):
            raise ValueError(f"expected a RESP array, got {line!r}")
        argc = int(line[1:])
        args: list[str] = []
        for _ in range(argc):
            hdr = self._line()
            if hdr is None or not hdr.startswith(b"$"):
                raise ValueError(f"expected a bulk string header, got {hdr!r}")
            data = self._bulk(int(hdr[1:]))
            if data is None:
                return None
            args.append(data.decode("utf-8"))
        return args


# --- Server ---------------------------------------------------------------

class RespServer:
    """Threaded RESP server. Use as a context manager; `port` is assigned."""

    def __init__(self, password: str | None = None, fragment: bool = True) -> None:
        self.backend = fake_redis.FakeRedis()
        self.password = password
        self.fragment = fragment
        self.commands: list[list[str]] = []   # every command received, in order
        self._lock = threading.Lock()
        self._sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self._sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self._sock.bind(("127.0.0.1", 0))
        self._sock.listen(8)
        self.port = self._sock.getsockname()[1]
        self._stop = threading.Event()
        self._threads: list[threading.Thread] = []

    def __enter__(self) -> "RespServer":
        t = threading.Thread(target=self._accept_loop, daemon=True)
        t.start()
        self._threads.append(t)
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.stop()

    def stop(self) -> None:
        self._stop.set()
        try:
            self._sock.close()
        except OSError:
            pass

    def commands_named(self, name: str) -> list[list[str]]:
        with self._lock:
            return [c for c in self.commands if c and c[0].upper() == name.upper()]

    # --- connection handling ----------------------------------------------

    def _accept_loop(self) -> None:
        while not self._stop.is_set():
            try:
                conn, _addr = self._sock.accept()
            except OSError:
                return
            t = threading.Thread(target=self._serve, args=(conn,), daemon=True)
            t.start()
            self._threads.append(t)

    def _send(self, conn: socket.socket, data: bytes) -> None:
        if not self.fragment or len(data) < 4:
            conn.sendall(data)
            return
        # Split into three pieces so the client must buffer across recv calls.
        a, b = len(data) // 3, 2 * len(data) // 3
        for part in (data[:a], data[a:b], data[b:]):
            if part:
                conn.sendall(part)

    def _serve(self, conn: socket.socket) -> None:
        reader = _CommandReader(conn)
        authed = self.password is None
        try:
            while not self._stop.is_set():
                try:
                    args = reader.read_command()
                except (OSError, ValueError):
                    return
                if args is None:
                    return
                with self._lock:
                    self.commands.append(args)

                cmd = args[0].upper()
                if cmd == "AUTH":
                    authed = args[-1] == self.password
                    self._send(conn, encode(OK) if authed
                               else encode(_Error("WRONGPASS invalid password")))
                    continue
                if not authed:
                    self._send(conn, encode(_Error("NOAUTH Authentication required.")))
                    continue

                try:
                    reply = self._dispatch(cmd, args[1:])
                except fake_redis.ResponseError as exc:
                    reply = _Error(str(exc))
                except Exception as exc:  # noqa: BLE001
                    reply = _Error(f"ERR {type(exc).__name__}: {exc}")
                self._send(conn, encode(reply))
        finally:
            try:
                conn.close()
            except OSError:
                pass

    # --- command dispatch --------------------------------------------------

    def _dispatch(self, cmd: str, a: list[str]) -> Any:  # noqa: C901
        b = self.backend

        if cmd == "PING":
            return _Simple("PONG")
        if cmd == "SELECT":
            return OK
        if cmd == "CLIENT":
            return OK
        if cmd == "INFO":
            return "# Server\r\nredis_version:7.2.4\r\nredis_mode:standalone\r\n"
        if cmd == "DEL":
            n = 0
            for key in a:
                if key in b.streams:
                    del b.streams[key]
                    b.groups.pop(key, None)
                    n += 1
            return n
        if cmd == "EXISTS":
            return sum(1 for k in a if k in b.streams)
        if cmd == "TTL":
            return -1

        if cmd == "XADD":
            key, rest = a[0], a[1:]
            maxlen = minid = None
            approximate = True
            i = 0
            while i < len(rest):
                tok = rest[i].upper()
                if tok in ("MAXLEN", "MINID"):
                    j = i + 1
                    if rest[j] in ("~", "="):
                        approximate = rest[j] == "~"
                        j += 1
                    if tok == "MAXLEN":
                        maxlen = int(rest[j])
                    else:
                        minid = rest[j]
                    i = j + 1
                elif tok == "NOMKSTREAM":
                    i += 1
                elif tok == "LIMIT":
                    i += 2
                else:
                    break
            entry_id, pairs = rest[i], rest[i + 1:]
            if len(pairs) % 2 != 0:
                raise fake_redis.ResponseError("ERR wrong number of arguments for 'xadd'")
            new_id = b.xadd(key, dict(zip(pairs[::2], pairs[1::2])), id=entry_id)
            if maxlen is not None:
                b.xtrim(key, maxlen=maxlen, approximate=approximate)
            elif minid is not None:
                b.xtrim(key, minid=minid, approximate=approximate)
            return new_id

        if cmd == "XTRIM":
            key, rest = a[0], a[1:]
            strategy = rest[0].upper()
            j = 1
            if rest[j] in ("~", "="):
                approximate = rest[j] == "~"
                j += 1
            else:
                approximate = True
            value = rest[j]
            if strategy == "MAXLEN":
                return b.xtrim(key, maxlen=int(value), approximate=approximate)
            if strategy == "MINID":
                return b.xtrim(key, minid=value, approximate=approximate)
            raise fake_redis.ResponseError("ERR syntax error")

        if cmd == "XLEN":
            return b.xlen(a[0])

        if cmd == "XRANGE":
            count = int(a[a.index("COUNT") + 1]) if "COUNT" in a else None
            return [[i, [x for kv in f.items() for x in kv]]
                    for i, f in b.xrange(a[0], a[1], a[2], count=count)]

        if cmd == "XDEL":
            key, ids = a[0], set(a[1:])
            before = len(b.streams.get(key, []))
            b.streams[key] = [e for e in b.streams.get(key, []) if e[0] not in ids]
            return before - len(b.streams[key])

        if cmd == "XGROUP":
            sub = a[0].upper()
            if sub == "CREATE":
                mkstream = any(x.upper() == "MKSTREAM" for x in a[4:])
                b.xgroup_create(a[1], a[2], id=a[3], mkstream=mkstream)
                return OK
            if sub == "DESTROY":
                return 1 if b.groups.get(a[1], {}).pop(a[2], None) is not None else 0
            raise fake_redis.ResponseError("ERR unknown XGROUP subcommand")

        if cmd == "XREADGROUP":
            if a[0].upper() != "GROUP":
                raise fake_redis.ResponseError("ERR syntax error")
            group, consumer = a[1], a[2]
            i, count, block, noack = 3, None, None, False
            while i < len(a):
                tok = a[i].upper()
                if tok == "COUNT":
                    count = int(a[i + 1]); i += 2
                elif tok == "BLOCK":
                    block = int(a[i + 1]); i += 2
                elif tok == "NOACK":
                    noack = True; i += 1
                elif tok == "STREAMS":
                    i += 1
                    break
                else:
                    raise fake_redis.ResponseError(f"ERR syntax error near {a[i]}")
            rest = a[i:]
            half = len(rest) // 2
            keys, ids = rest[:half], rest[half:]
            out = b.xreadgroup(group, consumer, dict(zip(keys, ids)),
                               count=count, noack=noack)
            if not out:
                return None  # nil, exactly what real Redis returns on timeout
            return [[name, [[i, [x for kv in f.items() for x in kv]]
                            for i, f in entries]] for name, entries in out]

        if cmd == "XACK":
            return b.xack(a[0], a[1], *a[2:])

        if cmd == "XPENDING":
            info = b.xpending(a[0], a[1])
            if info["pending"] == 0:
                return [0, None, None, None]
            group = b.groups[a[0]][a[1]]
            per: dict[str, int] = {}
            for meta in group.pending.values():
                per[meta["consumer"]] = per.get(meta["consumer"], 0) + 1
            return [info["pending"], info["min"], info["max"],
                    [[name, str(n)] for name, n in per.items()]]

        if cmd == "XAUTOCLAIM":
            key, group, consumer = a[0], a[1], a[2]
            min_idle, start = int(a[3]), a[4]
            i, count, justid = 5, None, False
            while i < len(a):
                if a[i].upper() == "COUNT":
                    count = int(a[i + 1]); i += 2
                elif a[i].upper() == "JUSTID":
                    justid = True; i += 1
                else:
                    i += 1
            cursor, claimed, deleted = b.xautoclaim(
                key, group, consumer, min_idle, start_id=start, count=count)
            if justid:
                return [cursor, [i for i, _f in claimed], deleted]
            return [cursor,
                    [[i, [x for kv in (f or {}).items() for x in kv]] for i, f in claimed],
                    deleted]

        if cmd == "XINFO":
            sub = a[0].upper()
            if sub == "STREAM":
                entries = b.streams.get(a[1], [])
                return ["length", len(entries),
                        "last-generated-id", entries[-1][0] if entries else "0-0"]
            if sub == "GROUPS":
                return [["name", g, "consumers", 1, "pending", len(grp.pending)]
                        for g, grp in b.groups.get(a[1], {}).items()]

        raise fake_redis.ResponseError(f"ERR unknown command '{cmd}'")
