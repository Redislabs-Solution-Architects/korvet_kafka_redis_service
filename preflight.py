
# !/usr/bin/env python3
"""Preflight check for the airgapped install.

Run this first on the client host. It verifies the Python version, that the
bundle is intact and dependency free, that the Redis endpoint is reachable, and
that the account can actually run every stream command this service needs. It
cleans up after itself.

    python preflight.py --host redis.example.com --port 12000

Exit code 0 means good to go; 1 means something listed above needs fixing.
"""

from __future__ import annotations

import argparse
import socket
import sys
import time

MIN_PYTHON = (3, 9)

OK = "  OK   "
BAD = " FAIL  "
WARN = " WARN  "

failures: list[str] = []
warnings: list[str] = []


def ok(msg: str) -> None:
    print(OK + msg)


def bad(msg: str, fix: str = "") -> None:
    print(BAD + msg + (f"\n         fix: {fix}" if fix else ""))
    failures.append(msg)


def warn(msg: str) -> None:
    print(WARN + msg)
    warnings.append(msg)


# --- 1. Python -------------------------------------------------------------

def check_python() -> None:
    print("\n1. Python runtime")
    v = sys.version_info
    if (v.major, v.minor) >= MIN_PYTHON:
        ok(f"Python {v.major}.{v.minor}.{v.micro} at {sys.executable}")
    else:
        bad(f"Python {v.major}.{v.minor} is too old, need "
            f"{MIN_PYTHON[0]}.{MIN_PYTHON[1]} or newer",
            "install a newer Python, or ask for a backported bundle")


# --- 2. Bundle integrity ---------------------------------------------------

def check_bundle() -> None:
    print("\n2. Bundle contents")
    import pathlib

    here = pathlib.Path(__file__).resolve().parent
    required = ["producer.py", "consumer.py", "payloads.py", "config.py",
                "redis_client.py"]
    missing = [f for f in required if not (here / f).exists()]
    if missing:
        bad(f"missing files: {', '.join(missing)}", "re-extract the archive")
    else:
        ok(f"all {len(required)} core modules present")

    # Every import must resolve from the standard library plus this folder.
    try:
        import config  # noqa: F401
        import consumer  # noqa: F401
        import payloads  # noqa: F401
        import producer  # noqa: F401
        import redis_client  # noqa: F401
        ok("all modules import with no third party packages")
    except Exception as exc:  # noqa: BLE001
        bad(f"import failed: {type(exc).__name__}: {exc}")
        return

    if "redis" in sys.modules:
        warn("the redis-py package is loaded; this bundle does not need it")

    # Generating a payload touches most of the generator's code paths.
    try:
        from payloads import generate_payload
        p = generate_payload(1)
        assert p["Fields"]["Main"]["ApplicationNo"]
        assert len(p["Fields"]["Product"]) == 3
        ok("payload generator produces a valid document")
    except Exception as exc:  # noqa: BLE001
        bad(f"payload generation failed: {type(exc).__name__}: {exc}")


# --- 3. Network ------------------------------------------------------------

def check_tcp(host: str, port: int, timeout: float) -> bool:
    print("\n3. Network reachability")
    started = time.monotonic()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            pass
    except OSError as exc:
        bad(f"cannot open a TCP connection to {host}:{port} -- {exc}",
            "check the endpoint, security group and host firewall")
        return False
    ok(f"TCP connect to {host}:{port} in {(time.monotonic() - started) * 1000:.0f}ms")
    return True


# --- 4. Redis ---------------------------------------------------------------

def check_redis(args: argparse.Namespace) -> None:
    print("\n4. Redis server")
    import config
    import redis_client
    from redis_client import RedisError, ResponseError

    kwargs = config.redis_kwargs()
    kwargs.update(host=args.host, port=args.port, db=args.db)
    if args.password:
        kwargs["password"] = args.password
    if args.username:
        kwargs["username"] = args.username
    if args.tls:
        kwargs["ssl"] = True
        kwargs["ssl_cert_reqs"] = "none" if args.tls_no_verify else "required"

    client = redis_client.Redis(**kwargs)

    try:
        if not client.ping():
            bad("PING did not return PONG")
            return
        ok("PING succeeded")
    except redis_client.AuthenticationError as exc:
        bad(f"authentication failed: {exc}",
            "pass --password (and --username if the database uses ACLs)")
        return
    except RedisError as exc:
        bad(f"PING failed: {exc}")
        return

    # Version gate: XTRIM MINID and XAUTOCLAIM both need 6.2 or newer.
    try:
        version = client.info("server").get("redis_version", "unknown")
        parts = tuple(int(x) for x in version.split(".")[:2])
        if parts >= (6, 2):
            ok(f"Redis {version} supports XTRIM MINID and XAUTOCLAIM")
        else:
            bad(f"Redis {version} is too old; XTRIM MINID needs 6.2 or newer",
                "upgrade Redis, or switch retention to MAXLEN with --ttl 0")
    except Exception:  # noqa: BLE001
        warn("could not read the server version from INFO")

    # Exercise every command the service uses, on a scratch key.
    probe = args.probe_key
    group = "preflight-group"
    try:
        client.delete(probe)

        entry_id = client.xadd(probe, {"payload": "{}", "probe": "1"})
        ok(f"XADD permitted (wrote {entry_id})")

        client.xgroup_create(probe, group, id="0", mkstream=True)
        ok("XGROUP CREATE permitted")

        resp = client.xreadgroup(group, "preflight", {probe: ">"}, count=1, block=100)
        if not resp or not resp[0][1]:
            bad("XREADGROUP returned nothing for an entry that was just written")
        else:
            ok("XREADGROUP permitted")
            got_id = resp[0][1][0][0]
            if client.xack(probe, group, got_id) == 1:
                ok("XACK permitted")
            else:
                bad("XACK did not acknowledge the entry")

        pending = client.xpending(probe, group)["pending"]
        if pending == 0:
            ok("XPENDING permitted, nothing left pending")
        else:
            warn(f"XPENDING reports {pending} still pending on the probe key")

        client.xautoclaim(probe, group, "preflight", 0, start_id="0-0", count=1)
        ok("XAUTOCLAIM permitted")

        # The retention mechanism itself, on its own key.
        #
        # Stream IDs must strictly increase, so a backdated ID cannot be added
        # to a stream that already holds a current entry: Redis answers "the ID
        # specified in XADD is equal or smaller than the target stream top item".
        # The aged entry therefore goes into a separate, empty key, oldest first.
        ttl_probe = f"{probe}:ttl"
        client.delete(ttl_probe)
        old_ms = int((time.time() - 3600) * 1000)
        client.xadd(ttl_probe, {"payload": "{}"}, id=f"{old_ms}-0")
        client.xadd(ttl_probe, {"payload": "{}"})  # current, must survive
        removed = client.xtrim(ttl_probe,
                               minid=int(time.time() * 1000) - config.TTL_SECONDS * 1000,
                               approximate=False)
        remaining = client.xlen(ttl_probe)
        if removed == 1 and remaining == 1:
            ok("XTRIM MINID permitted and effective "
               "(dropped the aged entry, kept the current one)")
        elif removed >= 1:
            ok(f"XTRIM MINID permitted (removed {removed}, {remaining} remaining)")
        else:
            warn("XTRIM MINID ran but removed nothing; check the server clock")
        client.delete(ttl_probe)

        # Clock skew between this host and Redis breaks age based trimming.
        server_ms = int(client.xadd(probe, {"payload": "{}"}).split("-")[0])
        skew = abs(server_ms - int(time.time() * 1000))
        if skew < 5_000:
            ok(f"clock skew vs Redis is {skew}ms")
        else:
            warn(f"clock skew vs Redis is {skew}ms; TTL trimming keys off the "
                 f"server clock, so a large skew shifts the retention window")

    except ResponseError as exc:
        # Only suggest a permissions fix when the error actually looks like one.
        # Blaming ACLs for an unrelated protocol error sends people to the wrong
        # place, which is worse than offering no hint at all.
        message = str(exc)
        if "NOPERM" in message or "no permissions" in message.lower():
            hint = ("the account needs read and write on the stream key, "
                    "plus permission for the XGROUP and XREADGROUP commands")
        elif "WRONGTYPE" in message:
            hint = (f"the key {probe!r} already exists and is not a stream; "
                    f"pass a different --probe-key")
        elif "unknown command" in message.lower():
            hint = "this Redis build does not support the stream commands"
        else:
            hint = "this is not a permissions problem; see the message above"
        bad(f"a stream command was rejected: {message}", hint)
    except RedisError as exc:
        bad(f"stream probe failed: {type(exc).__name__}: {exc}")
    finally:
        removed_keys = []
        for key in (probe, f"{probe}:ttl"):
            try:
                if client.delete(key):
                    removed_keys.append(key)
            except RedisError:
                warn(f"could not delete the probe key {key!r}, remove it manually")
        if removed_keys:
            ok("cleaned up probe key(s): " + ", ".join(repr(k) for k in removed_keys))
        client.close()

    # Warn if the real target stream already has a group with a backlog.
    try:
        client2 = redis_client.Redis(**kwargs)
        if client2.exists(args.stream):
            length = client2.xlen(args.stream)
            groups = client2.xinfo_groups(args.stream)
            warn(f"stream {args.stream!r} already exists with {length} entries "
                 f"and {len(groups)} group(s)")
            for g in groups:
                if int(g.get("pending", 0)) > 0:
                    warn(f"  group {g.get('name')!r} has {g.get('pending')} "
                         f"pending entries from a previous run")
        else:
            ok(f"target stream {args.stream!r} does not exist yet, will be created")
        client2.close()
    except RedisError:
        pass


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    import config

    p = argparse.ArgumentParser(
        description="Verify the host and Redis endpoint before running the service.",
        formatter_class=argparse.ArgumentDefaultsHelpFormatter,
    )
    p.add_argument("--host", default=config.REDIS_HOST)
    p.add_argument("--port", type=int, default=config.REDIS_PORT)
    p.add_argument("--db", type=int, default=config.REDIS_DB)
    p.add_argument("--username", default=config.REDIS_USERNAME)
    p.add_argument("--password", default=config.REDIS_PASSWORD)
    p.add_argument("--tls", action="store_true", default=config.REDIS_SSL)
    p.add_argument("--tls-no-verify", action="store_true",
                   default=not config.REDIS_SSL_VERIFY)
    p.add_argument("--stream", default=config.STREAM_KEY)
    p.add_argument("--probe-key", default="preflight:probe",
                   help="scratch key used for the permission probe, deleted afterwards")
    p.add_argument("--timeout", type=float, default=10.0)
    p.add_argument("--skip-redis", action="store_true",
                   help="only check the host and bundle, do not contact Redis")
    return p.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(argv)
    print("=" * 68)
    print("Redis stream service preflight")
    print("=" * 68)

    check_python()
    check_bundle()

    if not args.skip_redis:
        if check_tcp(args.host, args.port, args.timeout):
            check_redis(args)

    print("\n" + "=" * 68)
    if failures:
        print(f"{len(failures)} problem(s) must be fixed before running:")
        for f in failures:
            print(f"  - {f}")
        return 1
    if warnings:
        print(f"Ready to run, with {len(warnings)} warning(s):")
        for w in warnings:
            print(f"  - {w}")
        return 0
    print("All checks passed. Ready to run.")
    return 0


if __name__ == "__main__":
    sys.exit(main())

