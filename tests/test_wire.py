#!/usr/bin/env python3
"""Wire level tests for redis_client.py over a real TCP socket.

Runs the client against tests/resp_server.py, a genuine RESP server that records
every command it receives. This checks the two things a hand written client gets
wrong: the bytes sent for each command, and the parsing of each reply shape.

    python tests/test_wire.py
"""

from __future__ import annotations

import json
import sys
import threading
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

import redis_client  # noqa: E402
from redis_client import (  # noqa: E402
    AuthenticationError,
    DataError,
    Redis,
    ResponseError,
    _build_command,
)
from resp_server import RespServer  # noqa: E402

PASSED = 0
FAILED: list[str] = []


def check(label: str, condition: bool, detail: str = "") -> None:
    global PASSED
    if condition:
        PASSED += 1
        print(f"  PASS  {label}")
    else:
        FAILED.append(label)
        print(f"  FAIL  {label}" + (f" -- {detail}" if detail else ""))


def test_command_encoding() -> None:
    print("\n[RESP command encoding]")
    check("simple command is a RESP array of bulk strings",
          _build_command(["PING"]) == b"*1\r\n$4\r\nPING\r\n",
          repr(_build_command(["PING"])))
    check("integers are encoded as bulk strings",
          _build_command(["XTRIM", "k", "MAXLEN", 100])
          == b"*4\r\n$5\r\nXTRIM\r\n$1\r\nk\r\n$6\r\nMAXLEN\r\n$3\r\n100\r\n")
    # "Ångström" is 8 characters but 10 UTF-8 bytes; RESP counts bytes.
    check("utf-8 payloads are length prefixed in bytes not characters",
          _build_command(["SET", "k", "Ångström"]).split(b"\r\n")[5] == b"$10",
          repr(_build_command(["SET", "k", "Ångström"]).split(b"\r\n")[5]))
    for bad, why in [(True, "bool"), (None, "None"), ({"a": 1}, "dict")]:
        try:
            _build_command(["SET", "k", bad])
            check(f"{why} is rejected client side", False)
        except DataError:
            check(f"{why} is rejected client side", True)


def test_basic_roundtrip() -> None:
    print("\n[connect, PING, INFO over a real socket]")
    with RespServer() as srv:
        c = Redis(host="127.0.0.1", port=srv.port)
        check("PING returns True", c.ping() is True)
        info = c.info()
        check("INFO parses into a dict", info.get("redis_version") == "7.2.4",
              str(info)[:80])
        check("no SELECT sent for db 0", srv.commands_named("SELECT") == [])
        c.close()

        c2 = Redis(host="127.0.0.1", port=srv.port, db=3)
        c2.ping()
        check("SELECT sent for a non-zero db",
              srv.commands_named("SELECT") == [["SELECT", "3"]],
              str(srv.commands_named("SELECT")))
        c2.close()


def test_auth() -> None:
    print("\n[AUTH]")
    with RespServer(password="s3cret") as srv:
        c = Redis(host="127.0.0.1", port=srv.port, password="s3cret")
        check("correct password authenticates", c.ping() is True)
        check("AUTH sent with just the password",
              srv.commands_named("AUTH")[0] == ["AUTH", "s3cret"],
              str(srv.commands_named("AUTH")))
        c.close()

        c2 = Redis(host="127.0.0.1", port=srv.port, password="wrong")
        try:
            c2.ping()
            check("wrong password raises AuthenticationError", False)
        except AuthenticationError:
            check("wrong password raises AuthenticationError", True)
        c2.close()

        c3 = Redis(host="127.0.0.1", port=srv.port,
                   username="default", password="s3cret")
        c3.ping()
        check("AUTH sends username when supplied",
              ["AUTH", "default", "s3cret"] in srv.commands_named("AUTH"),
              str(srv.commands_named("AUTH")))
        c3.close()

        c4 = Redis(host="127.0.0.1", port=srv.port)
        try:
            c4.ping()
            check("missing password raises AuthenticationError", False)
        except AuthenticationError:
            check("missing password raises AuthenticationError", True)
        c4.close()


def test_xadd_encoding() -> None:
    print("\n[XADD on the wire]")
    with RespServer() as srv:
        c = Redis(host="127.0.0.1", port=srv.port)

        eid = c.xadd("s", {"a": "1", "b": "2"})
        check("XADD returns the generated id", "-" in eid, eid)
        sent = srv.commands_named("XADD")[0]
        check("XADD arg order is key, id, then field/value pairs",
              sent == ["XADD", "s", "*", "a", "1", "b", "2"], str(sent))

        c.xadd("s", {"a": "1"}, maxlen=5)
        check("MAXLEN uses the ~ marker by default",
              srv.commands_named("XADD")[1][2:5] == ["MAXLEN", "~", "5"],
              str(srv.commands_named("XADD")[1]))

        c.xadd("s", {"a": "1"}, minid=123, approximate=False)
        check("MINID with approximate=False uses the = marker",
              srv.commands_named("XADD")[2][2:5] == ["MINID", "=", "123"],
              str(srv.commands_named("XADD")[2]))

        c.xadd("s", {"a": "1"}, nomkstream=True)
        check("NOMKSTREAM precedes the trim options",
              srv.commands_named("XADD")[3][2] == "NOMKSTREAM",
              str(srv.commands_named("XADD")[3]))

        # Numeric and float field values must survive encoding.
        c.xadd("s", {"n": 42, "f": 1.5})
        got = dict(c.xrange("s")[-1][1])
        check("int and float field values round trip",
              got == {"n": "42", "f": "1.5"}, str(got))

        for bad, why in [({}, "empty fields")]:
            try:
                c.xadd("s", bad)
                check(f"{why} rejected", False)
            except DataError:
                check(f"{why} rejected", True)
        try:
            c.xadd("s", {"a": "1"}, maxlen=5, minid=1)
            check("maxlen and minid together rejected", False)
        except DataError:
            check("maxlen and minid together rejected", True)
        c.close()


def test_xtrim_encoding() -> None:
    print("\n[XTRIM on the wire]")
    with RespServer() as srv:
        c = Redis(host="127.0.0.1", port=srv.port)
        now_ms = int(time.time() * 1000)
        c.xadd("s", {"tag": "old"}, id=f"{now_ms - 1_200_000}-0")
        c.xadd("s", {"tag": "new"}, id=f"{now_ms}-0")

        removed = c.xtrim("s", minid=now_ms - 600_000, approximate=False)
        check("XTRIM MINID removes only the aged entry", removed == 1, str(removed))
        check("XTRIM MINID sends the = marker",
              srv.commands_named("XTRIM")[0] == ["XTRIM", "s", "MINID", "=",
                                                 str(now_ms - 600_000)],
              str(srv.commands_named("XTRIM")[0]))
        check("the fresh entry survives",
              [f["tag"] for _i, f in c.xrange("s")] == ["new"])

        c.xtrim("s", maxlen=0, approximate=True)
        check("XTRIM MAXLEN sends the ~ marker",
              srv.commands_named("XTRIM")[1] == ["XTRIM", "s", "MAXLEN", "~", "0"],
              str(srv.commands_named("XTRIM")[1]))

        for kwargs, why in [({}, "neither maxlen nor minid"),
                            ({"maxlen": 1, "minid": 1}, "both maxlen and minid")]:
            try:
                c.xtrim("s", **kwargs)
                check(f"{why} rejected", False)
            except DataError:
                check(f"{why} rejected", True)
        c.close()


def test_group_commands() -> None:
    print("\n[consumer group commands on the wire]")
    with RespServer() as srv:
        c = Redis(host="127.0.0.1", port=srv.port)
        for i in range(3):
            c.xadd("s", {"payload": json.dumps({"n": i})})

        check("XGROUP CREATE returns True",
              c.xgroup_create("s", "g", id="0", mkstream=True) is True)
        check("XGROUP CREATE sends MKSTREAM",
              srv.commands_named("XGROUP")[0]
              == ["XGROUP", "CREATE", "s", "g", "0", "MKSTREAM"],
              str(srv.commands_named("XGROUP")[0]))
        try:
            c.xgroup_create("s", "g", id="0", mkstream=True)
            check("a duplicate group raises ResponseError with BUSYGROUP", False)
        except ResponseError as exc:
            check("a duplicate group raises ResponseError with BUSYGROUP",
                  "BUSYGROUP" in str(exc), str(exc))

        resp = c.xreadgroup("g", "c1", {"s": ">"}, count=10, block=100)
        sent = srv.commands_named("XREADGROUP")[0]
        check("XREADGROUP arg order is GROUP g c COUNT n BLOCK ms STREAMS key id",
              sent == ["XREADGROUP", "GROUP", "g", "c1", "COUNT", "10",
                       "BLOCK", "100", "STREAMS", "s", ">"], str(sent))
        check("XREADGROUP reply parses to [[stream, [(id, {fields}), ...]]]",
              len(resp) == 1 and resp[0][0] == "s" and len(resp[0][1]) == 3,
              str(resp)[:120])
        entry_id, fields = resp[0][1][0]
        check("entry fields parse into a dict",
              isinstance(fields, dict) and "payload" in fields, str(fields)[:80])

        check("XPENDING reports 3 pending", c.xpending("s", "g")["pending"] == 3,
              str(c.xpending("s", "g")))
        check("XPENDING lists the consumer",
              c.xpending("s", "g")["consumers"] == [{"name": "c1", "pending": 3}],
              str(c.xpending("s", "g")["consumers"]))

        ids = [i for i, _f in resp[0][1]]
        check("XACK acks the whole batch in one call",
              c.xack("s", "g", *ids) == 3)
        check("XPENDING drops to 0 after ack", c.xpending("s", "g")["pending"] == 0)
        check("XACK with no ids is a no-op that sends nothing",
              c.xack("s", "g") == 0 and len(srv.commands_named("XACK")) == 1)

        empty = c.xreadgroup("g", "c1", {"s": ">"}, count=10, block=50)
        check("a nil XREADGROUP reply becomes an empty list", empty == [], str(empty))
        c.close()


def test_xautoclaim() -> None:
    print("\n[XAUTOCLAIM on the wire]")
    with RespServer() as srv:
        c = Redis(host="127.0.0.1", port=srv.port)
        c.xadd("s", {"payload": "{}"})
        c.xgroup_create("s", "g", id="0", mkstream=True)
        c.xreadgroup("g", "dead", {"s": ">"}, count=10)

        cursor, entries, deleted = c.xautoclaim("s", "g", "alive", 0,
                                                start_id="0-0", count=10)
        sent = srv.commands_named("XAUTOCLAIM")[0]
        check("XAUTOCLAIM arg order is key group consumer min-idle start COUNT n",
              sent == ["XAUTOCLAIM", "s", "g", "alive", "0", "0-0", "COUNT", "10"],
              str(sent))
        check("always returns a 3-tuple regardless of server version",
              isinstance(cursor, str) and isinstance(entries, list)
              and isinstance(deleted, list))
        check("the pending entry is claimed with its fields",
              len(entries) == 1 and entries[0][1] == {"payload": "{}"},
              str(entries))

        c.xautoclaim("s", "g", "alive", 0, justid=True)
        check("JUSTID is sent when requested",
              "JUSTID" in srv.commands_named("XAUTOCLAIM")[1])
        c.close()


def test_reply_shapes() -> None:
    print("\n[reply shape parsing]")
    with RespServer() as srv:
        c = Redis(host="127.0.0.1", port=srv.port)
        check("integer reply", c.xadd("s", {"a": "1"}) and c.xlen("s") == 1)
        check("nil bulk reply becomes None on an empty group read",
              c.xgroup_create("s", "g", id="$") and
              c.xreadgroup("g", "c", {"s": ">"}, block=10) == [])
        check("empty array reply", c.xrange("nosuchstream") == [])
        check("XINFO STREAM parses to a dict",
              c.xinfo_stream("s")["length"] == 1, str(c.xinfo_stream("s")))
        check("XINFO GROUPS parses to a list of dicts",
              c.xinfo_groups("s")[0]["name"] == "g", str(c.xinfo_groups("s")))
        check("DEL and EXISTS", c.exists("s") == 1 and c.delete("s") == 1
              and c.exists("s") == 0)

        try:
            c.execute_command("NOSUCHCOMMAND")
            check("unknown command raises ResponseError", False)
        except ResponseError:
            check("unknown command raises ResponseError", True)
        check("the connection is still usable after an error reply", c.ping() is True)
        c.close()


def test_fragmented_and_large() -> None:
    print("\n[fragmented replies and large payloads]")
    # The server splits every reply into three writes, so this path is already
    # exercised throughout; here we push a payload well past one TCP segment.
    with RespServer(fragment=True) as srv:
        c = Redis(host="127.0.0.1", port=srv.port)
        big = json.dumps({"blob": "x" * 300_000, "unicode": "Ångström" * 100})
        c.xadd("s", {"payload": big})
        got = c.xrange("s")[0][1]["payload"]
        check("a 300KB payload survives fragmentation intact", got == big,
              f"{len(got)} vs {len(big)}")
        check("multi-byte characters are not split mid-codepoint",
              json.loads(got)["unicode"].startswith("Ångström"))

        # Many commands back to back must not desynchronise the reader.
        for i in range(200):
            c.xadd("s2", {"i": str(i)})
        check("200 sequential commands stay in sync", c.xlen("s2") == 200,
              str(c.xlen("s2")))
        check("all 200 entries read back in order",
              [f["i"] for _i, f in c.xrange("s2")] == [str(i) for i in range(200)])
        c.close()


def test_reconnect() -> None:
    print("\n[connection loss and recovery]")
    with RespServer() as srv:
        c = Redis(host="127.0.0.1", port=srv.port)
        c.xadd("s", {"a": "1"})
        # Yank the socket out from under the client.
        c._sock.close()  # type: ignore[union-attr]
        try:
            c.xadd("s", {"a": "2"})
        except redis_client.RedisError:
            pass
        check("the client drops a broken socket", c._sock is None)
        check("the next call transparently reconnects",
              c.xadd("s", {"a": "3"}) is not None)
        check("state on the server is intact after reconnect", c.xlen("s") >= 2)
        c.close()

    # Connecting to a closed port must raise, not hang.
    try:
        Redis(host="127.0.0.1", port=srv.port, socket_connect_timeout=2.0).ping()
        check("a refused connection raises ConnectionError", False)
    except redis_client.ConnectionError:
        check("a refused connection raises ConnectionError", True)


def test_blocking_timeout() -> None:
    print("\n[BLOCK outlives the socket timeout]")
    with RespServer() as srv:
        # socket_timeout is deliberately shorter than BLOCK. A naive client would
        # raise TimeoutError here instead of waiting out the block.
        c = Redis(host="127.0.0.1", port=srv.port, socket_timeout=0.5)
        c.xgroup_create("s", "g", id="0", mkstream=True)
        started = time.monotonic()
        try:
            out = c.xreadgroup("g", "c", {"s": ">"}, count=1, block=1500)
            check("a long BLOCK does not trip the shorter socket timeout",
                  out == [], str(out))
        except redis_client.TimeoutError as exc:
            check("a long BLOCK does not trip the shorter socket timeout",
                  False, f"raised {exc}")
        check("the socket timeout is restored after the blocking call",
              abs((c._sock.gettimeout() or 0) - 0.5) < 0.01,
              str(c._sock.gettimeout()))
        _ = time.monotonic() - started
        check("the connection still works after a blocking read", c.ping() is True)
        c.close()


def test_from_url() -> None:
    print("\n[from_url]")
    c = redis_client.from_url("redis://user:pw%401@10.0.0.5:12000/2")
    check("host parsed", c.host == "10.0.0.5", c.host)
    check("port parsed", c.port == 12000, str(c.port))
    check("db parsed", c.db == 2, str(c.db))
    check("username parsed", c.username == "user", str(c.username))
    check("percent-encoded password decoded", c.password == "pw@1", str(c.password))
    check("plain redis:// is not TLS", c.ssl is False)
    check("rediss:// enables TLS", redis_client.from_url("rediss://h:1/0").ssl is True)
    try:
        redis_client.from_url("http://h:1/0")
        check("a non-redis scheme is rejected", False)
    except DataError:
        check("a non-redis scheme is rejected", True)


def test_producer_consumer_over_socket() -> None:
    print("\n[producer and consumer over a real socket]")
    import consumer as consumer_mod
    import producer as producer_mod

    with RespServer() as srv:
        pc = Redis(host="127.0.0.1", port=srv.port)
        cc = Redis(host="127.0.0.1", port=srv.port)

        prod = producer_mod.Producer(pc, "wire:test", ttl_seconds=600,
                                     max_len=0, trim_interval=0)
        cons = consumer_mod.Consumer(cc, "wire:test", "g1", "c1", batch_size=5,
                                     block_ms=100, claim_min_idle_ms=60000,
                                     claim_interval=999)
        cons.ensure_group()

        for i in range(30):
            prod.publish(__import__("payloads").generate_payload(i))
        check("30 payloads published over the socket", pc.xlen("wire:test") == 30,
              str(pc.xlen("wire:test")))

        total = 0
        while total < 30:
            resp = cc.xreadgroup("g1", "c1", {"wire:test": ">"}, count=5, block=100)
            if not resp:
                break
            cons._process_batch(resp[0][1], "wire")
            total += len(resp[0][1])
        check("all 30 consumed", cons.processed == 30, str(cons.processed))
        check("all 30 acked", cons.acked == 30, str(cons.acked))
        check("nothing pending", cons.pending_count() == 0, str(cons.pending_count()))

        # The document must survive the full JSON -> RESP -> JSON trip byte exact.
        original = __import__("payloads").generate_payload(99)
        prod.publish(original)
        raw = pc.xrange("wire:test")[-1][1]["payload"]
        check("the payload document round trips byte exact",
              json.loads(raw) == original)

        # TTL trim through the real client.
        now_ms = int(time.time() * 1000)
        pc.xadd("wire:ttl", {"payload": "{}"}, id=f"{now_ms - 900_000}-0")
        pc.xadd("wire:ttl", {"payload": "{}"}, id=f"{now_ms - 60_000}-0")
        p2 = producer_mod.Producer(pc, "wire:ttl", ttl_seconds=600, max_len=0,
                                   trim_interval=0)
        check("TTL trim over the socket removes only aged entries",
              p2.trim(force=True) == 1 and pc.xlen("wire:ttl") == 1)
        pc.close()
        cc.close()


def main() -> int:
    print("=" * 72)
    print("redis_client.py wire tests (real sockets, real RESP)")
    print("=" * 72)

    test_command_encoding()
    test_basic_roundtrip()
    test_auth()
    test_xadd_encoding()
    test_xtrim_encoding()
    test_group_commands()
    test_xautoclaim()
    test_reply_shapes()
    test_fragmented_and_large()
    test_reconnect()
    test_blocking_timeout()
    test_from_url()
    test_producer_consumer_over_socket()

    print("\n" + "=" * 72)
    if FAILED:
        print(f"{PASSED} passed, {len(FAILED)} FAILED")
        for label in FAILED:
            print(f"  - {label}")
        return 1
    print(f"All {PASSED} wire checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
