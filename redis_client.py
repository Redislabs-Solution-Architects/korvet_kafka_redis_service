"""Dependency-free Redis client, standard library only.

Exists so this service can be dropped onto an airgapped host with no pip install
step. It speaks RESP over a plain socket and implements only the commands this
project needs. Method names, keyword arguments and return shapes deliberately
match redis-py, so `producer.py` and `consumer.py` work unchanged against either.

Supported: PING, AUTH, SELECT, INFO, XADD, XLEN, XRANGE, XTRIM, XGROUP CREATE,
XREADGROUP, XACK, XPENDING, XAUTOCLAIM, XINFO STREAM/GROUPS, DEL, EXISTS, TTL.

Not a general purpose client. No pipelining, no pub/sub, no cluster redirection,
no connection pool. One socket per instance, which is what a single-threaded
producer or consumer loop actually needs.

Thread safety: a single instance is NOT safe to share across threads. Give each
thread its own client.
"""

from __future__ import annotations

import socket
import ssl as _ssl
from typing import Any, Iterable


# --- Exceptions, mirroring redis.exceptions -------------------------------

class RedisError(Exception):
    """Base class for every error this client raises."""


class ConnectionError(RedisError):  # noqa: A001 - matches redis-py's name
    """Socket level failure: refused, reset, timed out, or closed mid-reply."""


class TimeoutError(RedisError):  # noqa: A001 - matches redis-py's name
    """The socket timed out waiting for a reply."""


class ResponseError(RedisError):
    """The server returned an error reply, e.g. BUSYGROUP or NOGROUP."""


class AuthenticationError(ResponseError):
    """AUTH was rejected or required and not supplied."""


class DataError(RedisError):
    """Bad arguments caught client side before anything is sent."""


# --- Protocol ------------------------------------------------------------

CRLF = b"\r\n"


def _encode(value: Any) -> bytes:
    """Encode one command argument as bytes."""
    if isinstance(value, bytes):
        return value
    if isinstance(value, str):
        return value.encode("utf-8")
    if isinstance(value, bool):
        # Guard: bool is an int subclass, and sending "True" would be a bug.
        raise DataError("bool is not a valid Redis argument, pass 0 or 1")
    if isinstance(value, int):
        return str(value).encode("ascii")
    if isinstance(value, float):
        # repr avoids scientific notation surprises for typical money values.
        return repr(value).encode("ascii")
    if value is None:
        raise DataError("None is not a valid Redis argument")
    raise DataError(f"cannot encode {type(value).__name__} as a Redis argument")


def _build_command(args: Iterable[Any]) -> bytes:
    """Serialise a command as a RESP array of bulk strings."""
    parts = [_encode(a) for a in args]
    out = [b"*", str(len(parts)).encode("ascii"), CRLF]
    for p in parts:
        out += [b"$", str(len(p)).encode("ascii"), CRLF, p, CRLF]
    return b"".join(out)


class _Reader:
    """Incremental RESP reader over a socket.

    Buffers because a single recv can straddle replies or split one mid-token,
    which is easy to get wrong and shows up only under load.
    """

    def __init__(self, sock: socket.socket, encoding: str = "utf-8") -> None:
        self._sock = sock
        self._buf = bytearray()
        self._encoding = encoding

    def _fill(self) -> None:
        try:
            chunk = self._sock.recv(65536)
        except socket.timeout as exc:
            raise TimeoutError("timed out reading from Redis") from exc
        except OSError as exc:
            raise ConnectionError(f"socket error reading from Redis: {exc}") from exc
        if not chunk:
            raise ConnectionError("Redis closed the connection")
        self._buf.extend(chunk)

    def _read_line(self) -> bytes:
        while True:
            idx = self._buf.find(CRLF)
            if idx >= 0:
                line = bytes(self._buf[:idx])
                del self._buf[: idx + 2]
                return line
            self._fill()

    def _read_exactly(self, n: int) -> bytes:
        while len(self._buf) < n + 2:  # payload plus trailing CRLF
            self._fill()
        data = bytes(self._buf[:n])
        del self._buf[: n + 2]
        return data

    def read_reply(self, decode: bool = True) -> Any:
        line = self._read_line()
        if not line:
            raise ConnectionError("empty reply from Redis")
        kind, rest = line[:1], line[1:]

        # --- RESP2 ---
        if kind == b"+":  # simple string
            return rest.decode(self._encoding) if decode else rest
        if kind == b"-":  # error
            msg = rest.decode(self._encoding, "replace")
            if msg.startswith(("NOAUTH", "WRONGPASS", "ERR Client sent AUTH")):
                raise AuthenticationError(msg)
            raise ResponseError(msg)
        if kind == b":":  # integer
            return int(rest)
        if kind == b"$":  # bulk string
            length = int(rest)
            if length == -1:
                return None
            data = self._read_exactly(length)
            return data.decode(self._encoding) if decode else data
        if kind == b"*":  # array
            count = int(rest)
            if count == -1:
                return None
            return [self.read_reply(decode) for _ in range(count)]

        # --- RESP3 extras, in case the server negotiated it elsewhere ---
        if kind == b"_":  # null
            return None
        if kind == b"#":  # boolean
            return rest == b"t"
        if kind == b",":  # double
            return float(rest)
        if kind == b"(":  # big number
            return int(rest)
        if kind == b"!":  # blob error
            raise ResponseError(self._read_exactly(int(rest)).decode(self._encoding, "replace"))
        if kind == b"=":  # verbatim string
            data = self._read_exactly(int(rest))
            return data[4:].decode(self._encoding) if decode else data[4:]
        if kind in (b"%", b"|"):  # map / attribute
            pairs = [self.read_reply(decode) for _ in range(int(rest) * 2)]
            return dict(zip(pairs[::2], pairs[1::2]))
        if kind in (b"~", b">"):  # set / push
            return [self.read_reply(decode) for _ in range(int(rest))]

        raise ResponseError(f"unknown RESP type {kind!r} in reply {line!r}")


# --- Helpers for stream reply shapes -------------------------------------

def _pairs_to_dict(flat: list[Any] | None) -> dict[str, str]:
    if not flat:
        return {}
    return dict(zip(flat[::2], flat[1::2]))


def _parse_entries(raw: Any) -> list[tuple[str, dict[str, str] | None]]:
    """[[id, [f, v, ...]], ...] -> [(id, {f: v}), ...], preserving nils."""
    if not raw:
        return []
    out = []
    for item in raw:
        if item is None:
            continue
        entry_id, fields = item[0], item[1] if len(item) > 1 else None
        out.append((entry_id, _pairs_to_dict(fields) if fields is not None else None))
    return out


# --- Client --------------------------------------------------------------

class Redis:
    """Minimal Redis client. Constructor signature mirrors redis-py's."""

    def __init__(
        self,
        host: str = "127.0.0.1",
        port: int = 6379,
        db: int = 0,
        username: str | None = None,
        password: str | None = None,
        socket_timeout: float | None = None,
        socket_connect_timeout: float | None = 10.0,
        socket_keepalive: bool = True,
        decode_responses: bool = True,
        ssl: bool = False,
        ssl_ca_certs: str | None = None,
        ssl_cert_reqs: str = "required",
        client_name: str | None = None,
        **_ignored: Any,
    ) -> None:
        self.host = host
        self.port = port
        self.db = db
        self.username = username
        self.password = password
        self.socket_timeout = socket_timeout
        self.socket_connect_timeout = socket_connect_timeout
        self.socket_keepalive = socket_keepalive
        self.decode_responses = decode_responses
        self.ssl = ssl
        self.ssl_ca_certs = ssl_ca_certs
        self.ssl_cert_reqs = ssl_cert_reqs
        self.client_name = client_name

        self._sock: socket.socket | None = None
        self._reader: _Reader | None = None

    # --- connection management --------------------------------------------

    def _connect(self) -> None:
        try:
            sock = socket.create_connection(
                (self.host, self.port), timeout=self.socket_connect_timeout)
        except OSError as exc:
            raise ConnectionError(
                f"cannot connect to Redis at {self.host}:{self.port} -- {exc}") from exc

        if self.socket_keepalive:
            try:
                sock.setsockopt(socket.SOL_SOCKET, socket.SO_KEEPALIVE, 1)
            except OSError:
                pass
        sock.setsockopt(socket.IPPROTO_TCP, socket.TCP_NODELAY, 1)

        if self.ssl:
            ctx = _ssl.create_default_context(cafile=self.ssl_ca_certs)
            if self.ssl_cert_reqs == "none":
                # The PoC admin console uses a self-signed cert; databases may too.
                ctx.check_hostname = False
                ctx.verify_mode = _ssl.CERT_NONE
            sock = ctx.wrap_socket(sock, server_hostname=self.host)

        # Blocking reads use socket_timeout; XREADGROUP BLOCK overrides per call.
        sock.settimeout(self.socket_timeout)
        self._sock = sock
        self._reader = _Reader(sock)

        # Handshake, in the order redis-py uses.
        if self.password:
            if self.username:
                self.execute_command("AUTH", self.username, self.password)
            else:
                self.execute_command("AUTH", self.password)
        if self.client_name:
            try:
                self.execute_command("CLIENT", "SETNAME", self.client_name)
            except ResponseError:
                pass  # harmless if the server disallows it
        if self.db:
            self.execute_command("SELECT", self.db)

    def _ensure(self) -> None:
        if self._sock is None:
            self._connect()

    def close(self) -> None:
        if self._sock is not None:
            try:
                self._sock.close()
            except OSError:
                pass
        self._sock = None
        self._reader = None

    # redis-py aliases
    disconnect = close

    def __enter__(self) -> "Redis":
        self._ensure()
        return self

    def __exit__(self, *_exc: Any) -> None:
        self.close()

    # --- command execution ------------------------------------------------

    def execute_command(self, *args: Any, decode: bool | None = None,
                        read_timeout: float | None = -1.0) -> Any:
        """Send one command and read one reply.

        read_timeout of -1.0 means leave the socket timeout alone. Pass an
        explicit value for blocking commands so BLOCK can outlast socket_timeout.
        """
        self._ensure()
        assert self._sock is not None and self._reader is not None

        payload = _build_command(args)
        try:
            if read_timeout != -1.0:
                self._sock.settimeout(read_timeout)
            self._sock.sendall(payload)
            return self._reader.read_reply(
                self.decode_responses if decode is None else decode)
        except (ConnectionError, TimeoutError):
            # The stream position is unknown after a partial reply; drop the
            # socket so the next call reconnects cleanly rather than desyncing.
            self.close()
            raise
        except OSError as exc:
            self.close()
            raise ConnectionError(f"socket error talking to Redis: {exc}") from exc
        finally:
            if read_timeout != -1.0 and self._sock is not None:
                try:
                    self._sock.settimeout(self.socket_timeout)
                except OSError:
                    pass

    # --- generic ----------------------------------------------------------

    def ping(self) -> bool:
        return self.execute_command("PING") == "PONG"

    def info(self, section: str | None = None) -> dict[str, str]:
        raw = self.execute_command("INFO", section) if section else self.execute_command("INFO")
        out: dict[str, str] = {}
        for line in str(raw).splitlines():
            if line and not line.startswith("#") and ":" in line:
                k, _, v = line.partition(":")
                out[k] = v
        return out

    def delete(self, *names: str) -> int:
        return int(self.execute_command("DEL", *names))

    def exists(self, *names: str) -> int:
        return int(self.execute_command("EXISTS", *names))

    def ttl(self, name: str) -> int:
        return int(self.execute_command("TTL", name))

    # --- streams: writing -------------------------------------------------

    def xadd(self, name: str, fields: dict[str, Any], id: str = "*",  # noqa: A002
             maxlen: int | None = None, approximate: bool = True,
             nomkstream: bool = False, minid: int | str | None = None,
             limit: int | None = None) -> str:
        if not fields:
            raise DataError("XADD requires at least one field/value pair")
        if maxlen is not None and minid is not None:
            raise DataError("Only one of ``maxlen`` or ``minid`` may be specified")

        args: list[Any] = ["XADD", name]
        if nomkstream:
            args.append("NOMKSTREAM")
        if maxlen is not None:
            args += ["MAXLEN", "~" if approximate else "=", maxlen]
        elif minid is not None:
            args += ["MINID", "~" if approximate else "=", minid]
        if limit is not None:
            args += ["LIMIT", limit]
        args.append(id)
        for k, v in fields.items():
            args += [k, v]
        return str(self.execute_command(*args))

    def xtrim(self, name: str, maxlen: int | None = None, approximate: bool = True,
              minid: int | str | None = None, limit: int | None = None) -> int:
        if maxlen is not None and minid is not None:
            raise DataError("Only one of ``maxlen`` or ``minid`` may be specified")
        if maxlen is None and minid is None:
            raise DataError("One of ``maxlen`` or ``minid`` must be specified")

        args: list[Any] = ["XTRIM", name]
        if maxlen is not None:
            args += ["MAXLEN", "~" if approximate else "=", maxlen]
        else:
            args += ["MINID", "~" if approximate else "=", minid]
        if limit is not None:
            args += ["LIMIT", limit]
        return int(self.execute_command(*args))

    def xdel(self, name: str, *ids: str) -> int:
        return int(self.execute_command("XDEL", name, *ids))

    # --- streams: reading -------------------------------------------------

    def xlen(self, name: str) -> int:
        return int(self.execute_command("XLEN", name))

    def xrange(self, name: str, min: str = "-", max: str = "+",  # noqa: A002
               count: int | None = None) -> list[tuple[str, dict[str, str]]]:
        args: list[Any] = ["XRANGE", name, min, max]
        if count is not None:
            args += ["COUNT", count]
        return _parse_entries(self.execute_command(*args))  # type: ignore[return-value]

    def xinfo_stream(self, name: str) -> dict[str, Any]:
        return _pairs_to_dict(self.execute_command("XINFO", "STREAM", name))

    def xinfo_groups(self, name: str) -> list[dict[str, Any]]:
        return [_pairs_to_dict(g) for g in (self.execute_command("XINFO", "GROUPS", name) or [])]

    # --- streams: consumer groups ----------------------------------------

    def xgroup_create(self, name: str, groupname: str, id: str = "$",  # noqa: A002
                      mkstream: bool = False, entries_read: int | None = None) -> bool:
        args: list[Any] = ["XGROUP", "CREATE", name, groupname, id]
        if mkstream:
            args.append("MKSTREAM")
        if entries_read is not None:
            args += ["ENTRIESREAD", entries_read]
        return self.execute_command(*args) == "OK"

    def xgroup_destroy(self, name: str, groupname: str) -> int:
        return int(self.execute_command("XGROUP", "DESTROY", name, groupname))

    def xreadgroup(self, groupname: str, consumername: str, streams: dict[str, str],
                   count: int | None = None, block: int | None = None,
                   noack: bool = False) -> list:
        args: list[Any] = ["XREADGROUP", "GROUP", groupname, consumername]
        if count is not None:
            args += ["COUNT", count]
        if block is not None:
            args += ["BLOCK", block]
        if noack:
            args.append("NOACK")
        args.append("STREAMS")
        args += list(streams.keys())
        args += list(streams.values())

        # The socket must outlast BLOCK or we would time out on a healthy wait.
        read_timeout = -1.0 if block is None else (block / 1000.0) + 10.0
        raw = self.execute_command(*args, read_timeout=read_timeout)
        if not raw:
            return []
        # RESP3 returns a map of stream -> entries; RESP2 an array of pairs.
        if isinstance(raw, dict):
            return [[k, _parse_entries(v)] for k, v in raw.items()]
        return [[item[0], _parse_entries(item[1])] for item in raw]

    def xack(self, name: str, groupname: str, *ids: str) -> int:
        if not ids:
            return 0
        return int(self.execute_command("XACK", name, groupname, *ids))

    def xpending(self, name: str, groupname: str) -> dict[str, Any]:
        raw = self.execute_command("XPENDING", name, groupname)
        if not raw:
            return {"pending": 0, "min": None, "max": None, "consumers": []}
        consumers = [{"name": c[0], "pending": int(c[1])} for c in (raw[3] or [])]
        return {"pending": int(raw[0]), "min": raw[1], "max": raw[2], "consumers": consumers}

    def xautoclaim(self, name: str, groupname: str, consumername: str,
                   min_idle_time: int, start_id: str = "0-0",
                   count: int | None = None, justid: bool = False) -> list:
        args: list[Any] = ["XAUTOCLAIM", name, groupname, consumername,
                           min_idle_time, start_id]
        if count is not None:
            args += ["COUNT", count]
        if justid:
            args.append("JUSTID")
        raw = self.execute_command(*args)
        if not raw:
            return ["0-0", [], []]
        cursor = raw[0]
        entries = raw[1] if justid else _parse_entries(raw[1])
        deleted = raw[2] if len(raw) > 2 else []
        # Always 3 elements so callers need no version branching.
        return [cursor, entries, deleted]


# Convenience alias so `from redis_client import StrictRedis` also works.
StrictRedis = Redis


def from_url(url: str, **kwargs: Any) -> Redis:
    """Build a client from redis://[user:pass@]host:port/db (or rediss:// for TLS)."""
    from urllib.parse import unquote, urlparse

    parsed = urlparse(url)
    if parsed.scheme not in ("redis", "rediss"):
        raise DataError(f"unsupported scheme {parsed.scheme!r}, expected redis or rediss")
    path = (parsed.path or "").strip("/")
    kwargs.setdefault("host", parsed.hostname or "127.0.0.1")
    kwargs.setdefault("port", parsed.port or 6379)
    kwargs.setdefault("db", int(path) if path.isdigit() else 0)
    if parsed.username:
        kwargs.setdefault("username", unquote(parsed.username))
    if parsed.password:
        kwargs.setdefault("password", unquote(parsed.password))
    kwargs.setdefault("ssl", parsed.scheme == "rediss")
    return Redis(**kwargs)
