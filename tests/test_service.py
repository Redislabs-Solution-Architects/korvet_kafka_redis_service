#!/usr/bin/env python3
"""Self-contained checks for the producer, consumer and payload generator.

Runs against the in-memory fake in fake_redis.py, so no Redis server is needed:

    python tests/test_service.py

Covers payload schema fidelity against the sample document, the internal
consistency rules the generator promises, the XADD -> XREADGROUP -> XACK round
trip, MINID based TTL trimming, the MAXLEN safety cap, and XAUTOCLAIM recovery
of entries a crashed consumer left pending.
"""

from __future__ import annotations

import json
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))
sys.path.insert(0, str(Path(__file__).resolve().parent))

# Stand the fake in for redis_client before producer/consumer import it, so these
# tests exercise the service logic without any socket involved. Wire level
# behaviour of the real client is covered separately in test_wire.py.
import types  # noqa: E402

import fake_redis  # noqa: E402

_stub = types.ModuleType("redis_client")
_stub.Redis = fake_redis.FakeRedis  # type: ignore[attr-defined]
for _name in ("RedisError", "ResponseError", "ConnectionError", "DataError"):
    setattr(_stub, _name, getattr(fake_redis, _name))
sys.modules["redis_client"] = _stub

import consumer as consumer_mod  # noqa: E402
import producer as producer_mod  # noqa: E402
from payloads import generate_payload  # noqa: E402

STREAM = "test:applications"
GROUP = "test-group"

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


# --- the sample payload from the specification, used as the schema oracle ---

SAMPLE = json.loads(r"""
{"Fields":{"Main":{"ApplicationNo":"","AgentCode":"","AgreementNo":"","ApplicationDate":"",
"ApprovalDate":"","JudgementDate":"","AgreementDate":"","ValidityDate":"","FormType":0,
"IsNonMember":0,"IsEasyApply":1,"IsMember":1,"PrivacyNotice":1,"Date":"","FaxNo":"","MSGroup":""},
"App":{"Title":"","FullName":"","NewNRIC":"","OldNRIC":"","DOB":"","Gender":"","Race":"",
"MaritalStatus":"","ResidencyStatus":"","Nationality":"","PresentAddress1":"","PresentAddress2":"",
"PresentAddress3":"","PresentAddressPostCode":"","PresentAddressCity":"","PresentAddressState":"",
"PermAddress":"","PermAddressPostcode":"","HomeTelNo":"","HandphoneNo":"","EmailAddress":"",
"CorAddressHomeAddress":1,"CorAddressOffice":0,"NoOfDependent":2,"ResidentalStatus":"",
"YearsOfStay":5,"MonthsOfStay":6,"TIN":""},
"Emp":{"CompanyName":"","OfficeTelNo":"","OfficeTelNoExt":"","OfficeAddress":"",
"OfficeAddressPostcode":"","OccupationType":"","SSTRegistrationNo":"","BusinessNature":"",
"Position":"","Department":"","OtherIncome":0,"ServiceYears":0,"ServiceMonths":0,
"MontlySalary":0,"SalaryDate":"","MidMonthSalaryDate":"","SourceOfOtherIncome":""},
"Joint":{"HasJoint":1,"Title":"","FullName":"","NewNRIC":"","OldNRIC":"","DOB":"","Gender":"",
"Relationship":"","HandphoneNo":"","CompanyName":"","OfficeAddress":"","OfficeAddressPostCode":"",
"OfficeTelNo":"","OfficeTelNoExt":"","NatureofBusiness":"","Position":"","Department":"",
"ServiceYears":0,"ServiceMonths":0,"GrossMonthlySalary":0,"NetMonthlySalary":0,"Race":"",
"MaritalStatus":"","ResidencyStatus":"","PresentAddress":"","PresentAddressPostcode":""},
"Emergency":{"Name":"","Relationship":"","ResidentialAddress":"","ResidentialAddressPostCode":"",
"HomeTelNo":"","HandphoneNo":"","OfficeTelNo":"","OfficeTelNoExt":""},
"Product":[{"Brand":null,"Description":null,"Model":null,"CashPrice":0,"Status":null,"Remarks":null}],
"Finance":{"CashPrice":0,"Deposit":0,"CashDeposit":0,"NonCashDeposit":0,"NetCashPrice":0,
"Freight":0,"RegistrationFee":0,"Insurance":0,"TotalLessDeposit":0,"InterestRate":0,
"TotalInterest":0,"OriginalBalance":0,"AnnualPercRate":0,"AgreementPrice":0,"PriceDifference":0,
"NumMonths":0,"NumInstalments":0,"InstalmentAmt":0,"FinalInstalmentAmt":0,"StampDuty":0,
"EPPrice":0,"InitialPayment":0,"BalanceSum":0,"TotalCashPrice":0,"DownPayment":0,
"PromoVoucher":0,"PromoCode":"","PromoName":"","FinanceAmount":0,"FinanceCharges":0,
"MonthlyRate":0,"AdvInstalmentAmt":0,"Months":0,"ParticipationFees":0,"ParticipationFeesTax":0,
"ParticipationFeesTotal":0},
"Merchant":{"Name":"","Branch":"","Address1":"","Address2":"","Address3":""},
"Payment":{"Bank":"","Branch":"","UseSalaryAccount":1,"SalaryBankAccount":"",
"SalaryBankAccountCurrent":1,"SalaryBankAccountSavings":0,"Method":"","AccountHolder":"","ACNo":""},
"Declaration":{"Disclosure1":1,"Disclosure2":1,"PromoMaterial":0,"Invoice":1},
"Stamping":{"Method":"","Timestamp":"","RefNo":""}},
"RequestId":"","DocRequestCode":"","DocCode":""}
""")


def test_schema() -> None:
    print("\n[schema fidelity vs the sample document]")
    p = generate_payload(1)

    check("top level keys and order match",
          list(p) == list(SAMPLE), f"{list(p)} != {list(SAMPLE)}")
    check("Fields section keys and order match",
          list(p["Fields"]) == list(SAMPLE["Fields"]))

    for section in SAMPLE["Fields"]:
        if section == "Product":
            continue
        want, got = list(SAMPLE["Fields"][section]), list(p["Fields"][section])
        missing = [k for k in want if k not in got]
        extra = [k for k in got if k not in want]
        check(f"{section}: exact key set and order",
              want == got, f"missing={missing} extra={extra}")

    check("Product is a 3 element array",
          isinstance(p["Fields"]["Product"], list) and len(p["Fields"]["Product"]) == 3,
          str(len(p["Fields"]["Product"])))
    check("Product item keys match",
          all(list(item) == list(SAMPLE["Fields"]["Product"][0])
              for item in p["Fields"]["Product"]))
    check("payload is JSON serialisable",
          isinstance(json.dumps(p), str))


def test_consistency(n: int = 400) -> None:
    print(f"\n[internal consistency across {n} generated payloads]")
    bad: dict[str, str] = {}

    for _ in range(n):
        p = generate_payload()
        f = p["Fields"]["Finance"]
        app = p["Fields"]["App"]
        main = p["Fields"]["Main"]
        products = p["Fields"]["Product"]

        def near(a: float, b: float, tol: float = 0.02) -> bool:
            return abs(a - b) <= tol

        # Deposit split
        if not near(f["CashDeposit"] + f["NonCashDeposit"], f["Deposit"]):
            bad["deposit = cash + non-cash"] = f"{f['CashDeposit']}+{f['NonCashDeposit']}!={f['Deposit']}"
        # Net cash price
        if not near(f["CashPrice"] - f["Deposit"], f["NetCashPrice"]):
            bad["net cash price"] = str(f)
        # Product prices roll up to the cash price
        total = sum(item["CashPrice"] for item in products)
        if not near(total, f["CashPrice"]):
            bad["products sum to CashPrice"] = f"{total} != {f['CashPrice']}"
        # Financed amount
        expected_fin = (f["NetCashPrice"] + f["Freight"] + f["RegistrationFee"]
                        + f["Insurance"] - f["PromoVoucher"])
        if not near(expected_fin, f["FinanceAmount"]):
            bad["finance amount"] = f"{expected_fin} != {f['FinanceAmount']}"
        # Flat interest
        if not near(f["FinanceAmount"] * f["MonthlyRate"] / 100 * f["Months"],
                    f["TotalInterest"], 0.05):
            bad["flat interest"] = str(f)
        # Balance
        if not near(f["FinanceAmount"] + f["TotalInterest"], f["OriginalBalance"], 0.05):
            bad["original balance"] = str(f)
        # Instalment schedule sums exactly to the balance
        sched = f["InstalmentAmt"] * (f["NumInstalments"] - 1) + f["FinalInstalmentAmt"]
        if not near(sched, f["OriginalBalance"], 0.05):
            bad["instalment schedule sums to balance"] = f"{sched} != {f['OriginalBalance']}"
        if f["NumMonths"] != f["Months"] or f["NumInstalments"] != f["NumMonths"]:
            bad["term fields agree"] = str(f)
        # SST on participation fees
        if not near(f["ParticipationFees"] + f["ParticipationFeesTax"],
                    f["ParticipationFeesTotal"]):
            bad["participation fee total"] = str(f)
        if not near(f["ParticipationFees"] * 0.08, f["ParticipationFeesTax"]):
            bad["participation fee tax at 8%"] = str(f)
        # Non negative money
        for key in ("CashPrice", "Deposit", "NetCashPrice", "FinanceAmount",
                    "OriginalBalance", "InstalmentAmt", "FinalInstalmentAmt"):
            if f[key] < 0:
                bad[f"{key} non negative"] = str(f[key])

        # NRIC encodes DOB, and its parity encodes gender
        nric = app["NewNRIC"]
        dob = app["DOB"]
        if nric[:6] != f"{dob[8:10]}{dob[3:5]}{dob[0:2]}":
            bad["NRIC prefix matches DOB"] = f"{nric} vs {dob}"
        last = int(nric[-1])
        if (last % 2 == 1) != (app["Gender"] == "Male"):
            bad["NRIC parity matches gender"] = f"{nric} {app['Gender']}"
        if len(nric) != 14 or nric[6] != "-" or nric[9] != "-":
            bad["NRIC format"] = nric

        # Postcode belongs to the stated state
        from payloads import STATES
        pc = int(app["PresentAddressPostCode"])
        if not any(lo <= pc <= hi for lo, hi in STATES[app["PresentAddressState"]][1]):
            bad["postcode in state range"] = f"{pc} not in {app['PresentAddressState']}"

        # Date chain is monotonic
        from datetime import datetime
        fmt = "%d/%m/%Y %H:%M:%S"
        chain = [datetime.strptime(main[k], fmt) for k in
                 ("ApplicationDate", "ApprovalDate", "JudgementDate", "AgreementDate")]
        if chain != sorted(chain):
            bad["date chain monotonic"] = str([main[k] for k in
                ("ApplicationDate", "ApprovalDate", "JudgementDate", "AgreementDate")])
        if datetime.strptime(main["ValidityDate"], fmt) <= chain[-1]:
            bad["validity after agreement"] = main["ValidityDate"]
        if main["IsMember"] == main["IsNonMember"]:
            bad["member flags are exclusive"] = str(main["IsMember"])

        # Joint applicant
        j = p["Fields"]["Joint"]
        if j["HasJoint"] == 1 and j["NetMonthlySalary"] > j["GrossMonthlySalary"]:
            bad["joint net <= gross"] = f"{j['NetMonthlySalary']} > {j['GrossMonthlySalary']}"
        if j["HasJoint"] == 0 and j["FullName"] != "":
            bad["no joint means blank fields"] = j["FullName"]

        # Payment account flags
        pay = p["Fields"]["Payment"]
        if pay["UseSalaryAccount"] == 1 and (
                pay["SalaryBankAccountCurrent"] + pay["SalaryBankAccountSavings"] != 1):
            bad["salary account type is exclusive"] = str(pay)

    for label, detail in bad.items():
        check(label, False, detail)
    if not bad:
        for label in ("finance figures reconcile", "NRIC encodes DOB and gender",
                      "postcodes match state", "date chain monotonic",
                      "joint and payment flags coherent"):
            check(label, True)


def test_cheap_deal_edge_case(n: int = 4000) -> None:
    """Regression: a large promo voucher on a cheap item must not go negative.

    The cheapest catalogue item is 450 and the largest voucher is 600, so with a
    30% deposit the financed amount could previously fall below zero and drag
    interest, balance and every instalment negative with it. This hammers the
    generator hard enough to hit that corner reliably, and separately forces the
    exact worst case.
    """
    print(f"\n[cheap deal / large voucher edge case, {n} payloads]")
    worst = None
    negatives = 0
    for _ in range(n):
        f = generate_payload()["Fields"]["Finance"]
        for key in ("FinanceAmount", "OriginalBalance", "InstalmentAmt",
                    "FinalInstalmentAmt", "TotalInterest", "NetCashPrice",
                    "AgreementPrice", "EPPrice", "BalanceSum"):
            if f[key] < 0:
                negatives += 1
        if worst is None or f["FinanceAmount"] < worst["FinanceAmount"]:
            worst = f
    check("no negative money in any derived field", negatives == 0, f"{negatives} found")
    check("a voucher never exceeds the amount owed",
          worst["PromoVoucher"] <= worst["NetCashPrice"] + worst["Freight"]
          + worst["RegistrationFee"] + worst["Insurance"],
          str(worst))
    check("the smallest financed amount is still positive",
          worst["FinanceAmount"] > 0, str(worst["FinanceAmount"]))

    # Force the exact worst case rather than trusting it to come up by chance:
    # cheapest item, largest deposit, largest voucher.
    import payloads as _pl
    real_choice, real_randint, real_uniform = _pl.random.choice, _pl.random.randint, _pl.random.uniform
    try:
        def cheapest(seq):
            seq = list(seq)
            # Deposit percentage, promo tuple and voucher amount: always the
            # most punishing option available.
            if seq and isinstance(seq[0], float) and 0.0 in seq:
                return max(seq)
            if seq and isinstance(seq[0], tuple) and seq[0] and isinstance(seq[0][0], str):
                return seq[0]
            if seq and all(isinstance(x, int) for x in seq):
                return max(seq)
            return real_choice(seq)
        _pl.random.choice = cheapest
        _pl.random.randint = lambda a, b: a          # cheapest price in every range
        _pl.random.uniform = lambda a, b: a
        f = _pl._build_finance(450.0)                # the 450 microwave, alone
        check("forced worst case keeps FinanceAmount positive",
              f["FinanceAmount"] > 0, str(f["FinanceAmount"]))
        check("forced worst case keeps the instalment schedule positive",
              f["InstalmentAmt"] > 0 and f["FinalInstalmentAmt"] > 0,
              f"{f['InstalmentAmt']} / {f['FinalInstalmentAmt']}")
        check("forced worst case still reconciles",
              abs(f["FinanceAmount"] + f["TotalInterest"] - f["OriginalBalance"]) < 0.05,
              str(f))
    finally:
        _pl.random.choice, _pl.random.randint, _pl.random.uniform = (
            real_choice, real_randint, real_uniform)


def test_variability(n: int = 200) -> None:
    print(f"\n[randomisation across {n} payloads]")
    payloads = [generate_payload(i) for i in range(n)]
    ids = {p["RequestId"] for p in payloads}
    names = {p["Fields"]["App"]["FullName"] for p in payloads}
    amounts = {p["Fields"]["Finance"]["FinanceAmount"] for p in payloads}
    states = {p["Fields"]["App"]["PresentAddressState"] for p in payloads}
    joint = {p["Fields"]["Joint"]["HasJoint"] for p in payloads}

    check("RequestId unique per payload", len(ids) == n, f"{len(ids)}/{n}")
    check("applicant names vary", len(names) > 15, str(len(names)))
    check("finance amounts vary", len(amounts) > n * 0.8, str(len(amounts)))
    check("states vary", len(states) > 8, str(len(states)))
    check("both joint and single applications appear", joint == {0, 1}, str(joint))


def test_roundtrip() -> None:
    print("\n[XADD -> XREADGROUP -> XACK round trip]")
    client = fake_redis.FakeRedis()
    prod = producer_mod.Producer(client, STREAM, ttl_seconds=600, max_len=0,
                                 trim_interval=0)
    cons = consumer_mod.Consumer(client, STREAM, GROUP, "c1", batch_size=10,
                                 block_ms=0, claim_min_idle_ms=60000,
                                 claim_interval=999)
    cons.ensure_group()

    for i in range(25):
        prod.publish(generate_payload(i))
    check("25 entries written", client.xlen(STREAM) == 25, str(client.xlen(STREAM)))

    # Envelope carries the routing fields the consumer relies on.
    _first_id, fields = client.xrange(STREAM, count=1)[0]
    for key in ("payload", "request_id", "application_no", "produced_at_ms",
                "doc_request_code", "agreement_no", "schema_version"):
        check(f"envelope has {key}", key in fields)
    check("envelope payload parses back to the full document",
          json.loads(fields["payload"])["Fields"]["Main"]["ApplicationNo"]
          == fields["application_no"])

    resp = client.xreadgroup(GROUP, "c1", {STREAM: ">"}, count=100)
    cons._process_batch(resp[0][1], "test")
    check("all 25 processed", cons.processed == 25, str(cons.processed))
    check("all 25 acked", cons.acked == 25, str(cons.acked))
    check("nothing left pending", cons.pending_count() == 0, str(cons.pending_count()))

    # A second read sees nothing new.
    check("no redelivery after ack",
          client.xreadgroup(GROUP, "c1", {STREAM: ">"}, count=10) == [])

    # ensure_group is idempotent (BUSYGROUP path).
    try:
        cons.ensure_group()
        check("ensure_group tolerates an existing group", True)
    except Exception as exc:  # noqa: BLE001
        check("ensure_group tolerates an existing group", False, str(exc))


def test_ttl_trim() -> None:
    print("\n[TTL enforced with XTRIM MINID]")
    client = fake_redis.FakeRedis()
    prod = producer_mod.Producer(client, STREAM, ttl_seconds=600, max_len=0,
                                 trim_interval=0)

    now_ms = int(time.time() * 1000)
    # Three entries aged 20min, 11min and 5min. TTL is 600s (10min).
    client.xadd(STREAM, {"payload": "{}", "tag": "old-20m"}, id=f"{now_ms - 1200_000}-0")
    client.xadd(STREAM, {"payload": "{}", "tag": "old-11m"}, id=f"{now_ms - 660_000}-0")
    client.xadd(STREAM, {"payload": "{}", "tag": "fresh-5m"}, id=f"{now_ms - 300_000}-0")
    check("3 entries seeded", client.xlen(STREAM) == 3)

    removed = prod.trim(force=True)
    remaining = [f["tag"] for _i, f in client.xrange(STREAM)]
    check("both entries older than the TTL removed", removed == 2, str(removed))
    check("the entry inside the window survives", remaining == ["fresh-5m"], str(remaining))

    # Stream key still exists with its group intact, unlike EXPIRE on the key.
    client.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
    prod.trim(force=True)
    check("stream and group survive trimming", STREAM in client.streams
          and GROUP in client.groups.get(STREAM, {}))

    # Rate limiting: a second immediate trim is skipped when an interval is set.
    prod2 = producer_mod.Producer(client, STREAM, 600, 0, trim_interval=999)
    prod2.trim()
    before = prod2._last_trim
    prod2.trim()
    check("trim honours its interval", prod2._last_trim == before)


def test_maxlen_cap() -> None:
    print("\n[MAXLEN safety cap]")
    client = fake_redis.FakeRedis()
    prod = producer_mod.Producer(client, STREAM, ttl_seconds=600, max_len=10,
                                 trim_interval=0)
    for i in range(30):
        prod.publish(generate_payload(i))
    prod.trim(force=True)
    check("stream capped at MAXLEN", client.xlen(STREAM) == 10, str(client.xlen(STREAM)))
    check("the newest entries are the ones kept",
          client.xrange(STREAM)[-1][1]["request_id"].endswith("-029"),
          client.xrange(STREAM)[-1][1]["request_id"])


def test_autoclaim_recovery() -> None:
    print("\n[XAUTOCLAIM recovers entries a dead consumer left pending]")
    client = fake_redis.FakeRedis()
    prod = producer_mod.Producer(client, STREAM, 600, 0, 0)
    for i in range(5):
        prod.publish(generate_payload(i))

    dead = consumer_mod.Consumer(client, STREAM, GROUP, "dead-1", 10, 0, 0, 0)
    dead.ensure_group()
    # Read but never ack, simulating a crash mid-batch.
    client.xreadgroup(GROUP, "dead-1", {STREAM: ">"}, count=5)
    check("5 entries stuck pending", dead.pending_count() == 5, str(dead.pending_count()))

    # min_idle_time 0 so they are immediately eligible.
    alive = consumer_mod.Consumer(client, STREAM, GROUP, "alive-1", 10, 0,
                                  claim_min_idle_ms=0, claim_interval=0)
    alive.reclaim_stale()
    check("all 5 reclaimed and processed", alive.processed == 5, str(alive.processed))
    check("all 5 acked by the new consumer", alive.acked == 5, str(alive.acked))
    check("pending list drained", alive.pending_count() == 0, str(alive.pending_count()))

    # An idle threshold that has not elapsed must not steal live work.
    client2 = fake_redis.FakeRedis()
    prod2 = producer_mod.Producer(client2, STREAM, 600, 0, 0)
    prod2.publish(generate_payload(1))
    c1 = consumer_mod.Consumer(client2, STREAM, GROUP, "busy", 10, 0, 60000, 0)
    c1.ensure_group()
    client2.xreadgroup(GROUP, "busy", {STREAM: ">"}, count=1)
    c2 = consumer_mod.Consumer(client2, STREAM, GROUP, "thief", 10, 0,
                              claim_min_idle_ms=60000, claim_interval=0)
    c2.reclaim_stale()
    check("in-flight entries are not stolen early", c2.processed == 0, str(c2.processed))


def test_handler_robustness() -> None:
    print("\n[handler edge cases]")
    client = fake_redis.FakeRedis()
    cons = consumer_mod.Consumer(client, STREAM, GROUP, "c1", 10, 0, 60000, 999)
    cons.ensure_group()

    client.xadd(STREAM, {"request_id": "no-payload-field"})
    client.xadd(STREAM, {"payload": "{not json", "request_id": "bad-json"})
    client.xadd(STREAM, {"payload": json.dumps(generate_payload(1)), "request_id": "ok"})

    resp = client.xreadgroup(GROUP, "c1", {STREAM: ">"}, count=10)
    cons._process_batch(resp[0][1], "test")
    check("malformed entries are acked rather than looping forever",
          cons.acked == 3, str(cons.acked))
    check("nothing left pending after poison entries",
          cons.pending_count() == 0, str(cons.pending_count()))

    # A handler that returns False must leave the entry pending for retry.
    client2 = fake_redis.FakeRedis()
    cons2 = consumer_mod.Consumer(client2, STREAM, GROUP, "c1", 10, 0, 60000, 999)
    cons2.ensure_group()
    client2.xadd(STREAM, {"payload": json.dumps(generate_payload(1))})
    cons2.handle = lambda _i, _f: False  # type: ignore[method-assign]
    resp = client2.xreadgroup(GROUP, "c1", {STREAM: ">"}, count=10)
    cons2._process_batch(resp[0][1], "test")
    check("a rejected entry stays pending", cons2.pending_count() == 1,
          str(cons2.pending_count()))
    check("a rejected entry is counted as failed", cons2.failed == 1, str(cons2.failed))

    # A handler that raises must not kill the loop and must leave it pending.
    client3 = fake_redis.FakeRedis()
    cons3 = consumer_mod.Consumer(client3, STREAM, GROUP, "c1", 10, 0, 60000, 999)
    cons3.ensure_group()
    client3.xadd(STREAM, {"payload": json.dumps(generate_payload(1))})

    def boom(_i, _f):
        raise RuntimeError("downstream exploded")

    cons3.handle = boom  # type: ignore[method-assign]
    resp = client3.xreadgroup(GROUP, "c1", {STREAM: ">"}, count=10)
    cons3._process_batch(resp[0][1], "test")
    check("an exception leaves the entry pending", cons3.pending_count() == 1)
    check("an exception does not propagate out of the batch", cons3.failed == 1)


def main() -> int:
    print("=" * 72)
    print("Redis stream service checks (in-memory fake, no server required)")
    print("=" * 72)

    test_schema()
    test_consistency()
    test_cheap_deal_edge_case()
    test_variability()
    test_roundtrip()
    test_ttl_trim()
    test_maxlen_cap()
    test_autoclaim_recovery()
    test_handler_robustness()

    print("\n" + "=" * 72)
    if FAILED:
        print(f"{PASSED} passed, {len(FAILED)} FAILED")
        for label in FAILED:
            print(f"  - {label}")
        return 1
    print(f"All {PASSED} checks passed")
    return 0


if __name__ == "__main__":
    sys.exit(main())
