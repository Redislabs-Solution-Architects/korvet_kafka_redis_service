"""Randomised generator for the BigCreditBank application payload.

The output matches the sample document schema exactly: same keys, same nesting,
same ordering, same types. Field *spellings* from the sample are preserved
verbatim, including the inconsistent ones (``MontlySalary``, ``ResidentalStatus``,
``NatureofBusiness``, and the mixed ``Postcode``/``PostCode`` casing) because
those are an existing wire contract, not typos we get to fix here.

Randomisation is internally consistent rather than field-by-field noise:

* NRIC encodes the same date of birth, birth state and gender as the sibling
  fields, and the check digit parity matches ``Gender``.
* Postcodes, cities and telephone area codes all belong to the chosen state.
* Names, ``Race`` and ``Title`` agree with each other and with ``Gender``.
* The application date chain is monotonic and ``ValidityDate`` is one year
  after ``AgreementDate``.
* Every ``Finance`` figure is derived from the product prices and the chosen
  term, so deposits, instalments, interest and balances reconcile.

Standard library only, no third party data dependency.
"""

from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta
from typing import Any

DATE_FMT = "%d/%m/%Y %H:%M:%S"

# --- Reference data --------------------------------------------------------

# state -> (NRIC birth-state codes, postcode ranges, telephone area code, cities)
STATES: dict[str, tuple[list[str], list[tuple[int, int]], str, list[str]]] = {
    "Johor": (["01", "21", "22", "23", "24"], [(79000, 86999)], "07",
              ["Johor Bahru", "Batu Pahat", "Muar", "Kluang", "Segamat"]),
    "Kedah": (["02", "25", "26", "27"], [(5000, 9999)], "04",
              ["Alor Setar", "Sungai Petani", "Kulim", "Jitra"]),
    "Kelantan": (["03", "28", "29"], [(15000, 18999)], "09",
                 ["Kota Bharu", "Pasir Mas", "Tanah Merah"]),
    "Melaka": (["04", "30", "31"], [(75000, 78999)], "06",
               ["Melaka", "Alor Gajah", "Jasin"]),
    "Negeri Sembilan": (["05", "32", "33"], [(70000, 73999)], "06",
                        ["Seremban", "Port Dickson", "Nilai", "Bahau"]),
    "Pahang": (["06", "34", "35"], [(25000, 28999), (39000, 39999)], "09",
               ["Kuantan", "Temerloh", "Bentong", "Cameron Highlands"]),
    "Pulau Pinang": (["07", "40", "41"], [(10000, 14999)], "04",
                     ["George Town", "Bayan Lepas", "Bukit Mertajam", "Butterworth"]),
    "Perak": (["08", "42", "43", "44"], [(30000, 36999)], "05",
              ["Ipoh", "Taiping", "Teluk Intan", "Sitiawan"]),
    "Perlis": (["09", "45"], [(1000, 2999)], "04", ["Kangar", "Arau"]),
    "Selangor": (["10", "46", "47", "48"], [(40000, 48999), (63000, 68000)], "03",
                 ["Shah Alam", "Petaling Jaya", "Klang", "Subang Jaya", "Cyberjaya"]),
    "Terengganu": (["11", "49", "50"], [(20000, 24999)], "09",
                   ["Kuala Terengganu", "Dungun", "Kemaman"]),
    "Sabah": (["12", "51", "52", "53"], [(88000, 91999)], "088",
              ["Kota Kinabalu", "Sandakan", "Tawau", "Lahad Datu"]),
    "Sarawak": (["13", "54", "55", "56", "57"], [(93000, 98999)], "082",
                ["Kuching", "Miri", "Sibu", "Bintulu"]),
    "Wilayah Persekutuan": (["14", "58", "59"], [(50000, 60000)], "03",
                            ["Kuala Lumpur", "Bangsar", "Cheras", "Setapak"]),
    "Labuan": (["15", "60"], [(87000, 87999)], "087", ["Victoria"]),
    "Putrajaya": (["16"], [(62000, 62999)], "03", ["Putrajaya"]),
}

MOBILE_PREFIXES = ["010", "011", "012", "013", "014", "016", "017", "018", "019"]

MALAY_MALE = ["Ahmad Faizal", "Mohd Rizal", "Muhammad Hafiz", "Zulkifli", "Amirul Hakim",
              "Shahrul Nizam", "Khairul Anwar", "Mohd Syafiq"]
MALAY_FEMALE = ["Nurul Aina", "Siti Aisyah", "Farah Nadia", "Noraini", "Syazwani",
                "Hafizah", "Nur Alia", "Rosmawati"]
MALAY_SURNAME = ["bin Abdullah", "bin Ismail", "bin Hassan", "binti Rahman",
                 "binti Yusof", "binti Othman"]

CHINESE_MALE = ["Tan Wei Ming", "Lim Chee Keong", "Lee Kok Wai", "Ong Jian Hao",
                "Wong Chun Yip", "Cheah Ming Han", "Goh Zhi Wei"]
CHINESE_FEMALE = ["Chong Mei Ling", "Ng Sook Yee", "Teoh Hui Xin", "Lau Jia Wen",
                  "Yap Li Ying", "Khoo Pei Shan", "Foo Wan Ting"]

INDIAN_MALE = ["Rajesh Kumar", "Suresh Pillai", "Vignesh Raj", "Arun Muthusamy",
               "Prakash Subramaniam", "Dinesh Nair"]
INDIAN_FEMALE = ["Priya Devi", "Kavitha Ramasamy", "Shanti Krishnan", "Devi Anandan",
                 "Meena Sundaram", "Latha Govindasamy"]

RACES = ["Malay", "Chinese", "Indian", "Bumiputera Sabah", "Bumiputera Sarawak"]

STREET_NAMES = ["Jalan Mawar", "Jalan Melati", "Jalan Teratai", "Jalan Kenanga",
                "Jalan Cempaka", "Jalan Ampang", "Jalan Bukit Bintang", "Jalan Tun Razak",
                "Lorong Damai", "Persiaran Setia"]
TAMAN_NAMES = ["Taman Melati", "Taman Desa", "Taman Sri Muda", "Taman Bukit Indah",
               "Taman Universiti", "Taman Perindustrian", "Bandar Baru Sentul"]

COMPANY_PREFIX = ["ABC", "Sinar", "Maju", "Prima", "Nexus", "Cahaya", "Global", "Teknologi",
                  "Bintang", "Harmoni"]
COMPANY_SUFFIX = ["Technologies Sdn Bhd", "Industries Sdn Bhd", "Solutions Sdn Bhd",
                  "Trading Sdn Bhd", "Manufacturing Sdn Bhd", "Berhad", "Holdings Sdn Bhd"]

BUSINESS_NATURE = ["IT Services", "Manufacturing", "Retail Trade", "Construction",
                   "Banking", "Logistics", "Education", "Healthcare", "Oil and Gas",
                   "Telecommunications"]
OCCUPATION_TYPES = ["Executive", "Non-Executive", "Manager", "Senior Manager",
                    "Professional", "Technician", "Self Employed", "Government Servant"]
POSITIONS = ["Software Engineer", "Accounts Executive", "Sales Manager", "Technician",
             "Operations Executive", "Finance Manager", "Customer Service Officer",
             "Project Engineer", "Administrator"]
DEPARTMENTS = ["Development", "Finance", "Sales", "Operations", "Human Resources",
               "Information Technology", "Marketing", "Logistics"]
OTHER_INCOME_SOURCES = ["Freelancing", "Rental Income", "Part Time Business",
                        "Dividend Income", "Commission", "e-Hailing"]

TITLES_MALE = ["Mr", "Dr", "Ir"]
TITLES_FEMALE = ["Ms", "Mrs", "Madam", "Dr"]

MARITAL_STATUS = ["Single", "Married", "Divorced", "Widowed"]
RESIDENCY_STATUS = ["Citizen", "Permanent Resident", "Foreigner"]
RESIDENTIAL_STATUS = ["Owned", "Rented", "Staying with Parents", "Company Provided",
                      "Mortgaged"]
RELATIONSHIPS = ["Spouse", "Brother", "Sister", "Father", "Mother", "Friend", "Colleague"]

PRODUCTS = [
    ("Samsung", "Smart TV 55 inch UHD", "UA55AU7000", 2500, 4200),
    ("Samsung", "Side by Side Refrigerator 617L", "RS62R5001B4", 3800, 5600),
    ("LG", "Front Load Washer 10.5kg", "FV1450S3B", 2200, 3400),
    ("LG", "OLED TV 65 inch", "OLED65C3PSA", 6500, 9800),
    ("Panasonic", "Inverter Air Conditioner 1.5HP", "CS-PU12XKH", 1800, 2900),
    ("Daikin", "Inverter Air Conditioner 2.0HP", "FTKF50B", 2400, 3600),
    ("Sharp", "Microwave Oven 25L", "R-2201G", 450, 780),
    ("Apple", "MacBook Air 13 inch M3", "MRXQ3ZP", 4999, 6499),
    ("Apple", "iPhone 15 Pro 256GB", "MTQV3ZP", 5499, 6999),
    ("Dyson", "Cordless Vacuum V15", "SV22", 2800, 3900),
    ("Sony", "Bravia XR 55 inch", "XR-55X90L", 4200, 5800),
    ("Electrolux", "Built-in Oven 72L", "KOAAS31X", 3100, 4400),
]
PRODUCT_STATUS = ["New", "Display Unit", "Refurbished"]
PRODUCT_REMARKS = ["Promotion bundle included", "Extended warranty 2 years",
                   "Free delivery and installation", "Trade-in applied", None]

MERCHANTS = [
    ("Bright Home Electrical Sdn Bhd", ["Central Gateway", "Valley Point", "Riverside Plaza", "Lakeside City Mall"]),
    ("Sinaran Electric Sdn Bhd", ["Bukit Bintang", "Setia Alam", "Ipoh Central"]),
    ("Northview Living Malaysia", ["Garden Court", "Bayview Mall", "Seaview Plaza"]),
    ("Titan Home Berhad", ["Cheras", "Klang", "Seremban 2"]),
    ("Metro Gadget Sdn Bhd", ["Orchid Mall KL", "Valley Point", "Johor Bahru City Centre"]),
]
MALL_ADDRESSES = [
    ("Lot 12, Ground Floor", "Valley Point Megamall", "Lingkaran Syed Putra"),
    ("Unit F-25, First Floor", "Riverside Plaza Mall", "Jalan PJS 11/15"),
    ("Lot G-08, Ground Floor", "Lakeside City Mall", "Lebuh Utama, Lakeside Resort City"),
    ("Lot 3-14, Third Floor", "Orchid Mall Kuala Lumpur", "168 Jalan Bukit Bintang"),
    ("Unit L2-30, Level 2", "Bayview Mall", "100 Persiaran Bayan Indah"),
]

BANKS = [
    ("Meridian Bank", ["Valley Point", "Jalan Tun Perak", "Shah Alam", "Penang Main"]),
    ("Crescent Bank", ["Bangsar", "Kota Damansara", "Ipoh Garden"]),
    ("Unity Bank", ["Taman Melawati", "Sri Petaling", "Johor Jaya"]),
    ("Harbour Bank", ["Jalan Ampang", "Puchong", "Kuching Central"]),
    ("Highland Bank", ["Damansara Uptown", "Cheras Selatan", "Melaka Raya"]),
    ("Bank Sejahtera", ["Putrajaya", "Kota Bharu", "Alor Setar"]),
]
PAYMENT_METHODS = ["Auto Debit", "Standing Instruction", "Direct Debit", "Salary Deduction"]

PROMOS = [
    ("PROMO20OFF", "20% Discount"),
    ("RAYA2026MY", "Raya Cashback"),
    ("ZEROINT12", "0% Interest 12 Months"),
    ("MEGASALE50", "Mega Sale Rebate"),
    ("NEWCUST100", "New Customer Voucher"),
    ("", ""),
]

MS_GROUPS = ["GroupA", "GroupB", "GroupC", "GroupD"]
STAMPING_METHODS = ["eKYC", "Manual", "eStamping", "Digital Signature"]
EMAIL_DOMAINS = ["example.com", "gmail.com", "yahoo.com", "outlook.com", "hotmail.com"]

SST_RATE = 0.08  # Malaysian service tax on participation fees


# --- Small helpers ---------------------------------------------------------

def _fmt(dt: datetime) -> str:
    return dt.strftime(DATE_FMT)


def _postcode(state: str) -> str:
    ranges = STATES[state][1]
    lo, hi = random.choice(ranges)
    return f"{random.randint(lo, hi):05d}"


def _landline(state: str) -> str:
    area = STATES[state][2]
    digits = 7 if len(area) == 2 else 6
    return f"{area}-{random.randint(10 ** (digits - 1), 10 ** digits - 1)}"


def _mobile() -> str:
    return f"{random.choice(MOBILE_PREFIXES)}-{random.randint(1000000, 9999999)}"


def _new_nric(dob: datetime, state: str, gender: str) -> str:
    """Malaysian NRIC: YYMMDD-PB-###G, G odd for male and even for female."""
    state_code = random.choice(STATES[state][0])
    serial = random.randint(0, 999)
    # Force the parity of the final digit to encode gender.
    last = random.randrange(1, 10, 2) if gender == "Male" else random.randrange(0, 10, 2)
    return f"{dob:%y%m%d}-{state_code}-{serial:03d}{last}"


def _old_nric() -> str:
    return f"{random.choice('ABCDEFGHK')}{random.randint(1000000, 9999999)}"


def _person(gender: str, race: str) -> str:
    if race == "Chinese":
        return random.choice(CHINESE_MALE if gender == "Male" else CHINESE_FEMALE)
    if race == "Indian":
        return random.choice(INDIAN_MALE if gender == "Male" else INDIAN_FEMALE)
    given = random.choice(MALAY_MALE if gender == "Male" else MALAY_FEMALE)
    surnames = [s for s in MALAY_SURNAME
                if s.startswith("bin " if gender == "Male" else "binti ")]
    return f"{given} {random.choice(surnames)}"


def _title(gender: str, marital: str) -> str:
    if gender == "Male":
        return random.choice(TITLES_MALE)
    if marital == "Married":
        return random.choice(["Mrs", "Madam", "Dr"])
    return random.choice(TITLES_FEMALE)


def _email(full_name: str) -> str:
    parts = [p for p in full_name.lower().replace(",", "").split()
             if p not in ("bin", "binti")]
    handle = ".".join(parts[:2]) if len(parts) >= 2 else parts[0]
    return f"{handle}{random.randint(1, 999)}@{random.choice(EMAIL_DOMAINS)}"


def _street_address(state: str) -> str:
    return (f"{random.randint(1, 499)}, {random.choice(STREET_NAMES)}, "
            f"{random.choice(TAMAN_NAMES)}, {random.choice(STATES[state][3])}")


def _company() -> str:
    return f"{random.choice(COMPANY_PREFIX)} {random.choice(COMPANY_SUFFIX)}"


def _day_in_month(when: datetime, day: int) -> datetime:
    """Set the day of month, clamped to the last valid day.

    Guards against ValueError in short months: asking for the 30th in February
    yields the 28th or 29th rather than blowing up.
    """
    import calendar

    last = calendar.monthrange(when.year, when.month)[1]
    return when.replace(day=min(day, last), hour=0, minute=0, second=0, microsecond=0)


def _money(value: float) -> float:
    """Round to sen and drop a pointless trailing .0 so ints stay ints."""
    rounded = round(value + 0.0, 2)
    return int(rounded) if rounded == int(rounded) else rounded


# --- Section builders ------------------------------------------------------

def _build_products() -> tuple[list[dict[str, Any]], float]:
    """Always three slots, 1-3 filled, unused slots nulled out as in the sample."""
    filled = random.choices([1, 2, 3], weights=[6, 3, 1])[0]
    chosen = random.sample(PRODUCTS, filled)
    products: list[dict[str, Any]] = []
    total = 0.0
    for brand, desc, model, lo, hi in chosen:
        price = _money(random.randint(lo, hi))
        total += price
        products.append({
            "Brand": brand,
            "Description": desc,
            "Model": model,
            "CashPrice": price,
            "Status": random.choice(PRODUCT_STATUS),
            "Remarks": random.choice(PRODUCT_REMARKS),
        })
    for _ in range(3 - filled):
        products.append({
            "Brand": None, "Description": None, "Model": None,
            "CashPrice": 0, "Status": None, "Remarks": None,
        })
    return products, total


def _build_finance(cash_price: float) -> dict[str, Any]:
    """Derive every figure from the product total and the chosen term.

    Flat-rate hire purchase, the way these agreements are actually quoted:
    interest is charged on the full financed amount for the whole term.
    """
    months = random.choice([6, 12, 18, 24, 36, 48, 60])

    deposit_pct = random.choice([0.0, 0.10, 0.15, 0.20, 0.30])
    deposit = _money(cash_price * deposit_pct)
    cash_deposit = _money(deposit * random.choice([1.0, 0.5, 0.0]))
    non_cash_deposit = _money(deposit - cash_deposit)

    net_cash_price = _money(cash_price - deposit)
    freight = _money(random.choice([0, 50, 100, 200, 500]))
    registration_fee = _money(random.choice([0, 100, 200]))
    insurance = _money(random.choice([0, 300, 500, 800, 1200]))

    # Subtotal owed before any voucher is applied.
    subtotal = _money(net_cash_price + freight + registration_fee + insurance)

    promo_code, promo_name = random.choice(PROMOS)
    promo_voucher = 0
    if promo_code:
        # A voucher can never exceed what is actually owed, and a 600 voucher
        # against a 450 microwave is not a real promotion either. Cap at 10% of
        # the subtotal. Without this cap a large voucher on a cheap single-item
        # deal drives FinanceAmount negative, and every figure derived from it
        # (interest, balance, instalments) goes negative with it.
        cap = _money(subtotal * 0.10)
        promo_voucher = min(_money(random.choice([0, 100, 200, 300, 600])), cap)

    finance_amount = _money(subtotal - promo_voucher)

    monthly_rate = round(random.uniform(0.30, 0.75), 2)      # percent per month
    interest_rate = _money(round(monthly_rate * 12, 2))       # flat annual percent
    finance_charges = _money(finance_amount * monthly_rate / 100 * months)
    total_interest = finance_charges
    original_balance = _money(finance_amount + total_interest)

    # Standard flat-to-effective approximation for a level-payment loan.
    annual_perc_rate = _money(round(monthly_rate * 24 * months / (months + 1), 2))

    instalment_amt = _money(round(original_balance / months, 2))
    # Last instalment absorbs the rounding drift so the schedule sums exactly.
    final_instalment_amt = _money(original_balance - instalment_amt * (months - 1))
    adv_instalment_amt = _money(instalment_amt) if random.random() < 0.4 else 0

    agreement_price = _money(deposit + original_balance)
    stamp_duty = _money(random.choice([10, 100, 200]))
    ep_price = _money(agreement_price + stamp_duty)
    price_difference = _money(agreement_price - cash_price)

    participation_fees = _money(random.choice([0, 50, 100, 150]))
    participation_fees_tax = _money(participation_fees * SST_RATE)
    participation_fees_total = _money(participation_fees + participation_fees_tax)

    return {
        "CashPrice": _money(cash_price),
        "Deposit": deposit,
        "CashDeposit": cash_deposit,
        "NonCashDeposit": non_cash_deposit,
        "NetCashPrice": net_cash_price,
        "Freight": freight,
        "RegistrationFee": registration_fee,
        "Insurance": insurance,
        "TotalLessDeposit": net_cash_price,
        "InterestRate": interest_rate,
        "TotalInterest": total_interest,
        "OriginalBalance": original_balance,
        "AnnualPercRate": annual_perc_rate,
        "AgreementPrice": agreement_price,
        "PriceDifference": price_difference,
        "NumMonths": months,
        "NumInstalments": months,
        "InstalmentAmt": instalment_amt,
        "FinalInstalmentAmt": final_instalment_amt,
        "StampDuty": stamp_duty,
        "EPPrice": ep_price,
        "InitialPayment": deposit,
        "BalanceSum": _money(original_balance - adv_instalment_amt),
        "TotalCashPrice": _money(cash_price),
        "DownPayment": deposit,
        "PromoVoucher": promo_voucher,
        "PromoCode": promo_code,
        "PromoName": promo_name,
        "FinanceAmount": finance_amount,
        "FinanceCharges": finance_charges,
        "MonthlyRate": monthly_rate,
        "AdvInstalmentAmt": adv_instalment_amt,
        "Months": months,
        "ParticipationFees": participation_fees,
        "ParticipationFeesTax": participation_fees_tax,
        "ParticipationFeesTotal": participation_fees_total,
    }


def _build_joint(app_state: str) -> dict[str, Any]:
    """Roughly 55% of applications carry a joint applicant."""
    if random.random() < 0.45:
        return {
            "HasJoint": 0,
            "Title": "", "FullName": "", "NewNRIC": "", "OldNRIC": "", "DOB": "",
            "Gender": "", "Relationship": "", "HandphoneNo": "", "CompanyName": "",
            "OfficeAddress": "", "OfficeAddressPostCode": "", "OfficeTelNo": "",
            "OfficeTelNoExt": "", "NatureofBusiness": "", "Position": "",
            "Department": "", "ServiceYears": 0, "ServiceMonths": 0,
            "GrossMonthlySalary": 0, "NetMonthlySalary": 0, "Race": "",
            "MaritalStatus": "", "ResidencyStatus": "", "PresentAddress": "",
            "PresentAddressPostcode": "",
        }

    gender = random.choice(["Male", "Female"])
    race = random.choice(RACES)
    marital = random.choice(MARITAL_STATUS)
    dob = datetime.now() - timedelta(days=random.randint(21 * 365, 58 * 365))
    state = random.choice(list(STATES)) if random.random() < 0.3 else app_state
    gross = _money(random.randint(2000, 20000))

    return {
        "HasJoint": 1,
        "Title": _title(gender, marital),
        "FullName": _person(gender, race),
        "NewNRIC": _new_nric(dob, state, gender),
        "OldNRIC": _old_nric(),
        "DOB": _fmt(dob.replace(hour=0, minute=0, second=0, microsecond=0)),
        "Gender": gender,
        "Relationship": random.choice(RELATIONSHIPS),
        "HandphoneNo": _mobile(),
        "CompanyName": _company(),
        "OfficeAddress": _street_address(state),
        "OfficeAddressPostCode": _postcode(state),
        "OfficeTelNo": _landline(state),
        "OfficeTelNoExt": str(random.randint(100, 999)),
        "NatureofBusiness": random.choice(BUSINESS_NATURE),
        "Position": random.choice(POSITIONS),
        "Department": random.choice(DEPARTMENTS),
        "ServiceYears": random.randint(0, 25),
        "ServiceMonths": random.randint(0, 11),
        "GrossMonthlySalary": gross,
        # Net is gross less statutory deductions, so always strictly lower.
        "NetMonthlySalary": _money(gross * random.uniform(0.82, 0.94)),
        "Race": race,
        "MaritalStatus": marital,
        "ResidencyStatus": random.choice(RESIDENCY_STATUS),
        "PresentAddress": _street_address(state),
        "PresentAddressPostcode": _postcode(state),
    }


# --- Public API ------------------------------------------------------------

def generate_payload(seq: int | None = None) -> dict[str, Any]:
    """Build one fully randomised, internally consistent application payload."""
    now = datetime.now()

    # --- Applicant identity, all mutually consistent ---
    gender = random.choice(["Male", "Female"])
    race = random.choice(RACES)
    marital = random.choice(MARITAL_STATUS)
    dob = (now - timedelta(days=random.randint(19 * 365, 60 * 365))).replace(
        hour=0, minute=0, second=0, microsecond=0)
    state = random.choice(list(STATES))
    city = random.choice(STATES[state][3])
    full_name = _person(gender, race)
    residency = random.choice(RESIDENCY_STATUS)

    # --- Date chain: application -> approval -> judgement -> agreement ---
    application_date = now - timedelta(days=random.randint(0, 30),
                                       hours=random.randint(0, 23),
                                       minutes=random.randint(0, 59))
    approval_date = application_date + timedelta(hours=random.randint(1, 48))
    judgement_date = approval_date + timedelta(hours=random.randint(1, 48))
    agreement_date = judgement_date + timedelta(hours=random.randint(1, 72))
    validity_date = (agreement_date + timedelta(days=365)).replace(
        hour=0, minute=0, second=0, microsecond=0)

    # --- Employment ---
    emp_state = state if random.random() < 0.75 else random.choice(list(STATES))
    monthly_salary = _money(random.randint(1800, 25000))
    has_other_income = random.random() < 0.45

    # --- Products drive the whole finance block ---
    products, cash_price_total = _build_products()
    finance = _build_finance(cash_price_total)

    merchant_name, branches = random.choice(MERCHANTS)
    addr1, addr2, addr3 = random.choice(MALL_ADDRESSES)
    bank_name, bank_branches = random.choice(BANKS)

    use_salary_account = random.choice([0, 1])
    account_no = "".join(str(random.randint(0, 9)) for _ in range(random.choice([10, 12, 16])))
    is_current = random.choice([0, 1])

    is_member = random.choice([0, 1])
    seq_no = seq if seq is not None else random.randint(1, 999)

    return {
        "Fields": {
            "Main": {
                "ApplicationNo": f"APP{now:%Y%m%d}{random.randint(1, 9999):04d}",
                "AgentCode": f"AGT{random.randint(100000, 999999)}",
                "AgreementNo": f"AGR{random.randint(100000, 999999)}",
                "ApplicationDate": _fmt(application_date),
                "ApprovalDate": _fmt(approval_date),
                "JudgementDate": _fmt(judgement_date),
                "AgreementDate": _fmt(agreement_date),
                "ValidityDate": _fmt(validity_date),
                "FormType": random.choice([0, 1, 2]),
                "IsNonMember": 0 if is_member else 1,
                "IsEasyApply": random.choice([0, 1]),
                "IsMember": is_member,
                "PrivacyNotice": 1,
                "Date": _fmt(application_date - timedelta(hours=random.randint(1, 12))),
                "FaxNo": _landline(state),
                "MSGroup": random.choice(MS_GROUPS),
            },
            "App": {
                "Title": _title(gender, marital),
                "FullName": full_name,
                "NewNRIC": _new_nric(dob, state, gender),
                "OldNRIC": _old_nric() if random.random() < 0.5 else "",
                "DOB": _fmt(dob),
                "Gender": gender,
                "Race": race,
                "MaritalStatus": marital,
                "ResidencyStatus": residency,
                "Nationality": "Malaysian" if residency != "Foreigner" else random.choice(
                    ["Singaporean", "Indonesian", "Indian", "Chinese", "Bangladeshi"]),
                "PresentAddress1": f"{random.randint(1, 499)}, {random.choice(STREET_NAMES)}",
                "PresentAddress2": random.choice(TAMAN_NAMES),
                "PresentAddress3": city,
                "PresentAddressPostCode": _postcode(state),
                "PresentAddressCity": city,
                "PresentAddressState": state,
                "PermAddress": f"{random.randint(1, 499)}, {random.choice(STREET_NAMES)}",
                "PermAddressPostcode": _postcode(state),
                "HomeTelNo": _landline(state),
                "HandphoneNo": _mobile(),
                "EmailAddress": _email(full_name),
                "CorAddressHomeAddress": 1,
                "CorAddressOffice": 0,
                "NoOfDependent": 0 if marital == "Single" else random.randint(0, 6),
                "ResidentalStatus": random.choice(RESIDENTIAL_STATUS),
                "YearsOfStay": random.randint(0, 30),
                "MonthsOfStay": random.randint(0, 11),
                "TIN": f"TIN{random.randint(100000000, 999999999)}",
            },
            "Emp": {
                "CompanyName": _company(),
                "OfficeTelNo": _landline(emp_state),
                "OfficeTelNoExt": str(random.randint(100, 999)),
                "OfficeAddress": (f"Level {random.randint(1, 30)}, "
                                  f"Menara {random.choice(COMPANY_PREFIX)}, "
                                  f"{random.choice(STREET_NAMES)}"),
                "OfficeAddressPostcode": _postcode(emp_state),
                "OccupationType": random.choice(OCCUPATION_TYPES),
                "SSTRegistrationNo": f"SST{random.randint(100000, 999999)}",
                "BusinessNature": random.choice(BUSINESS_NATURE),
                "Position": random.choice(POSITIONS),
                "Department": random.choice(DEPARTMENTS),
                "OtherIncome": _money(random.randint(200, 5000)) if has_other_income else 0,
                "ServiceYears": random.randint(0, 30),
                "ServiceMonths": random.randint(0, 11),
                "MontlySalary": monthly_salary,
                "SalaryDate": _fmt(_day_in_month(now, random.choice([25, 26, 28, 30]))),
                "MidMonthSalaryDate": _fmt(_day_in_month(now, 15)),
                "SourceOfOtherIncome": (random.choice(OTHER_INCOME_SOURCES)
                                        if has_other_income else ""),
            },
            "Joint": _build_joint(state),
            "Emergency": {
                "Name": _person(random.choice(["Male", "Female"]), race),
                "Relationship": random.choice(RELATIONSHIPS),
                "ResidentialAddress": _street_address(state),
                "ResidentialAddressPostCode": _postcode(state),
                "HomeTelNo": _landline(state),
                "HandphoneNo": _mobile(),
                "OfficeTelNo": _landline(state),
                "OfficeTelNoExt": str(random.randint(100, 999)),
            },
            "Product": products,
            "Finance": finance,
            "Merchant": {
                "Name": merchant_name,
                "Branch": random.choice(branches),
                "Address1": addr1,
                "Address2": addr2,
                "Address3": addr3,
            },
            "Payment": {
                "Bank": bank_name,
                "Branch": random.choice(bank_branches),
                "UseSalaryAccount": use_salary_account,
                "SalaryBankAccount": account_no if use_salary_account else "",
                "SalaryBankAccountCurrent": is_current if use_salary_account else 0,
                "SalaryBankAccountSavings": (1 - is_current) if use_salary_account else 0,
                "Method": random.choice(PAYMENT_METHODS),
                "AccountHolder": full_name,
                "ACNo": account_no,
            },
            "Declaration": {
                "Disclosure1": random.choice([0, 1]),
                "Disclosure2": random.choice([0, 1]),
                "PromoMaterial": random.choice([0, 1]),
                "Invoice": random.choice([0, 1]),
            },
            "Stamping": {
                "Method": random.choice(STAMPING_METHODS),
                "Timestamp": _fmt(agreement_date),
                "RefNo": str(random.randint(10000000, 99999999)),
            },
        },
        "RequestId": (f"{uuid.uuid4().hex[:7]}-{uuid.uuid4().hex[:4]}-"
                      f"{uuid.uuid4().hex[:7]}-99OF-{now:%Y%m%d}-{seq_no:03d}"),
        "DocRequestCode": random.choice(["West-Bank", "East-Bank", "North-Bank", "South-Bank"]),
        "DocCode": "",
    }


if __name__ == "__main__":
    import json

    print(json.dumps(generate_payload(1), indent=4, ensure_ascii=False))
