"""
dbf_reader.py — Reads Marg ERP's local DBF files and extracts new sales.

HOW MARG STORES DATA:
    Marg ERP (Visual FoxPro era) stores all data as DBF files in C:\\MARG\\DATA\\
    Three files matter for MediLoop:

    MDIS.DBF  — Sales header (one row per bill)
                Key fields: BILLNO, DATE/BDATE, CNAME, MOBILE, PARTY
    MDID.DBF  — Sales detail (one row per medicine per bill)
                Key fields: BILLNO, INAME/ITEM, QTY, RATE, STRIPS

    MMAS.DBF  — Party/customer master (phone numbers sometimes only here)
                Key fields: ACODE, ANAME, MOBILE, PHONE

    Marg v9 and v10 use slightly different field names — this reader
    tries multiple field name variants for each column.

WHY DBF:
    Marg was built in the FoxPro era. DBF is its native format.
    No password, no special driver — Python's dbfread reads them directly.
    The chemist doesn't need to "export" anything — we read live data.

ENCODING:
    Marg DBF files use Latin-1 (cp1252) encoding — NOT UTF-8.
    Always open with encoding="latin-1" or you'll get garbage on Hindi names.
"""

import logging
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Field name variants across Marg v9 / v10 / v11 ──────────────────────────
# Marg changed field names between major versions. We try all known variants.

BILL_FIELD_MAP = {
    # Bill number
    "bill_no":   ["BILLNO", "BILL_NO", "VCHNO", "INVNO", "DOCNO"],
    # Bill date
    "date":      ["DATE", "BDATE", "BILL_DATE", "VDATE", "DOC_DATE"],
    # Customer name
    "cust_name": ["CNAME", "CUST_NAME", "PARTY", "ANAME", "C_NAME", "CUSTNAME"],
    # Customer phone
    "phone":     ["MOBILE", "PHONE", "MOB", "CPHONE", "MOBILE1", "PH"],
    # Party code (used to join MMAS.DBF for phone if not on bill)
    "party_code":["ACODE", "PARTY_CODE", "PCODE", "CCODE"],
}

DETAIL_FIELD_MAP = {
    # Bill number (foreign key to MDIS)
    "bill_no":   ["BILLNO", "BILL_NO", "VCHNO", "INVNO", "DOCNO"],
    # Medicine/item name
    "item_name": ["INAME", "ITEM", "ITEM_NAME", "MNAME", "PRODUCT", "PROD_NAME", "PNAME"],
    # Quantity in strips
    "qty":       ["QTY", "STRIPS", "QUANTITY", "SQTY", "SALEQTY"],
    # Rate per strip
    "rate":      ["RATE", "MRP", "PRICE", "BRATE", "SRATE"],
    # Batch number (optional, useful for dedup)
    "batch":     ["BATCH", "BATCHNO", "BATCH_NO", "LOT"],
}

PARTY_FIELD_MAP = {
    "party_code": ["ACODE", "CODE", "PCODE"],
    "name":       ["ANAME", "NAME", "PARTY_NAME"],
    "phone":      ["MOBILE", "PHONE", "MOB", "MOBILE1"],
}

# Items to skip — non-medicine products common in Marg exports
SKIP_ITEM_KEYWORDS = [
    "surgical", "cosmetic", "diaper", "soap", "shampoo",
    "sanitizer", "gloves", "syringe", "cotton", "bandage",
    "baby", "powder", "oil", "lotion", "cream",   # skip cosmetics
    # Add more after seeing real Marg exports from your chemists
]


def _get_field(record, candidates: list) -> str:
    """Try each candidate field name and return the first non-empty value."""
    for field in candidates:
        val = record.get(field)
        if val is not None:
            val = str(val).strip()
            if val and val.lower() not in ("none", "null", "0", ""):
                return val
    return ""


def _parse_marg_date(raw) -> Optional[date]:
    """
    Parse Marg's date field — can be a Python date, datetime, or string.
    Marg stores dates as Python date objects when read via dbfread.
    """
    if raw is None:
        return None
    if isinstance(raw, (date, datetime)):
        return raw.date() if isinstance(raw, datetime) else raw
    # Try string parsing
    raw = str(raw).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _normalize_phone(raw: str) -> str:
    """Strip country code, spaces, dashes. Return 10-digit string or empty."""
    import re
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    return digits[-10:] if len(digits) >= 10 else ""


def _is_medicine(item_name: str) -> bool:
    """Return False if item looks like a non-medicine product."""
    lower = item_name.lower()
    return not any(kw in lower for kw in SKIP_ITEM_KEYWORDS)


def _estimate_refill_days(qty_strips: int) -> int:
    """
    Estimate refill cycle from quantity in strips.
    Assumption: 1 strip = 10 tablets, 1 tablet/day (OD) → 10 days/strip.
    Cap at 90 days min 7 days.
    """
    if qty_strips <= 0:
        return 30
    days = qty_strips * 10
    return max(7, min(90, days))


# ── Main reader function ──────────────────────────────────────────────────────

def read_new_sales(marg_data_path: str, since_date: date) -> list[dict]:
    """
    Read Marg ERP DBF files and return new sales since since_date.

    Args:
        marg_data_path: Path to Marg DATA folder, e.g. "C:/MARG/DATA"
        since_date:     Only return bills from this date onward

    Returns:
        List of dicts matching MediLoop's BillRecord shape:
        {
            "patient_name": str,
            "phone": str,          # 10-digit
            "medicines": [...],
            "source": "marg_dbf",
            "raw_invoice_id": str,
        }
    """
    try:
        import dbfread
    except ImportError:
        logger.error(
            "dbfread not installed. Run: pip install dbfread"
        )
        return []

    data_path = Path(marg_data_path)

    # ── Load MDIS.DBF (sales headers) ────────────────────────────────────────
    mdis_path = _find_file(data_path, ["MDIS.DBF", "mdis.dbf", "Mdis.dbf"])
    if not mdis_path:
        logger.error(
            "MDIS.DBF not found in Marg data folder",
            extra={"path": str(data_path)}
        )
        return []

    # ── Load MDID.DBF (sales detail / medicine lines) ─────────────────────────
    mdid_path = _find_file(data_path, ["MDID.DBF", "mdid.dbf", "Mdid.dbf"])
    if not mdid_path:
        logger.error(
            "MDID.DBF not found — cannot read medicine line items",
            extra={"path": str(data_path)}
        )
        return []

    # ── Load MMAS.DBF (party master for phone lookup) — optional ─────────────
    mmas_path = _find_file(data_path, ["MMAS.DBF", "mmas.dbf", "Mmas.dbf"])
    party_phone_map: dict[str, str] = {}
    if mmas_path:
        try:
            mmas = dbfread.DBF(str(mmas_path), encoding="latin-1", load=True)
            for rec in mmas:
                code = _get_field(rec, PARTY_FIELD_MAP["party_code"])
                phone = _normalize_phone(_get_field(rec, PARTY_FIELD_MAP["phone"]))
                if code and phone:
                    party_phone_map[code] = phone
            logger.info(
                "Party master loaded",
                extra={"party_count": len(party_phone_map)}
            )
        except Exception as e:
            logger.warning(
                "Could not load MMAS.DBF — phone fallback disabled",
                extra={"error": str(e)}
            )

    # ── Parse MDID.DBF → group medicines by bill number ───────────────────────
    bill_medicines: dict[str, list] = {}
    try:
        mdid = dbfread.DBF(str(mdid_path), encoding="latin-1", load=True)
        for rec in mdid:
            bill_no = _get_field(rec, DETAIL_FIELD_MAP["bill_no"])
            if not bill_no:
                continue

            item_name = _get_field(rec, DETAIL_FIELD_MAP["item_name"])
            if not item_name or not _is_medicine(item_name):
                continue

            # Parse quantity
            qty_raw = _get_field(rec, DETAIL_FIELD_MAP["qty"])
            try:
                qty = int(float(qty_raw)) if qty_raw else 1
            except ValueError:
                qty = 1

            # Parse rate
            rate_raw = _get_field(rec, DETAIL_FIELD_MAP["rate"])
            try:
                rate = float(rate_raw) if rate_raw else None
            except ValueError:
                rate = None

            # Extract dosage from item name (e.g. "Metformin 500mg" → "500mg")
            import re
            dosage_match = re.search(
                r"(\d+\.?\d*\s*(?:mg|ml|mcg|g|iu|units?))",
                item_name,
                re.IGNORECASE
            )
            dosage = dosage_match.group(1).strip() if dosage_match else ""

            if bill_no not in bill_medicines:
                bill_medicines[bill_no] = []

            bill_medicines[bill_no].append({
                "name":             item_name.strip().title(),
                "dosage":           dosage,
                "quantity":         qty,
                "refill_days":      _estimate_refill_days(qty),
                "price_per_strip":  rate,
            })

        logger.info(
            "MDID.DBF parsed",
            extra={"bills_with_medicines": len(bill_medicines)}
        )
    except Exception as e:
        logger.error("Failed to parse MDID.DBF", extra={"error": str(e)})
        return []

    # ── Parse MDIS.DBF → filter by date, join medicines ───────────────────────
    results = []
    bills_seen = 0
    bills_skipped_date = 0
    bills_skipped_no_phone = 0
    bills_skipped_no_meds = 0

    try:
        mdis = dbfread.DBF(str(mdis_path), encoding="latin-1", load=True)

        for rec in mdis:
            bills_seen += 1

            # Parse date
            raw_date = _get_field(rec, BILL_FIELD_MAP["date"])
            if not raw_date:
                raw_date = rec.get("DATE") or rec.get("BDATE")
            bill_date = _parse_marg_date(raw_date)

            if bill_date is None or bill_date < since_date:
                bills_skipped_date += 1
                continue

            bill_no = _get_field(rec, BILL_FIELD_MAP["bill_no"])

            # Get medicines for this bill
            medicines = bill_medicines.get(bill_no, [])
            if not medicines:
                bills_skipped_no_meds += 1
                continue

            # Get customer name
            cust_name = _get_field(rec, BILL_FIELD_MAP["cust_name"])
            if not cust_name or cust_name.lower() in ("cash", "retail", "walk-in", "counter", ""):
                bills_skipped_no_phone += 1
                continue

            # Get phone — try bill first, then party master lookup
            phone_raw = _get_field(rec, BILL_FIELD_MAP["phone"])
            phone = _normalize_phone(phone_raw)

            if not phone:
                party_code = _get_field(rec, BILL_FIELD_MAP["party_code"])
                phone = party_phone_map.get(party_code, "")

            if not phone or len(phone) != 10:
                bills_skipped_no_phone += 1
                continue

            # Add sale_date to each medicine line
            for med in medicines:
                med["sale_date"] = bill_date.isoformat()

            results.append({
                "patient_name":   cust_name.strip().title(),
                "phone":          phone,
                "medicines":      medicines,
                "source":         "marg_dbf",
                "raw_invoice_id": bill_no,
            })

    except Exception as e:
        logger.error("Failed to parse MDIS.DBF", extra={"error": str(e)})
        return []

    logger.info(
        "Marg DBF read complete",
        extra={
            "bills_seen":              bills_seen,
            "bills_returned":          len(results),
            "skipped_old_date":        bills_skipped_date,
            "skipped_no_phone":        bills_skipped_no_phone,
            "skipped_no_medicines":    bills_skipped_no_meds,
        }
    )

    return results


def _find_file(folder: Path, candidates: list[str]) -> Optional[Path]:
    """Find a file trying multiple case variants."""
    for name in candidates:
        p = folder / name
        if p.exists():
            return p
    return None


def discover_marg_path() -> Optional[str]:
    """
    Auto-detect Marg DATA folder on Windows.
    Checks the most common install locations.
    Returns path string or None if not found.
    """
    candidates = [
        r"C:\MARG\DATA",
        r"C:\Marg\DATA",
        r"C:\marg\data",
        r"D:\MARG\DATA",
        r"C:\MARGDATA",
        r"C:\MARG9\DATA",
        r"C:\MARG10\DATA",
        r"C:\Program Files\Marg\DATA",
        r"C:\Program Files (x86)\Marg\DATA",
    ]
    for path in candidates:
        p = Path(path)
        # Check for MDIS.DBF — confirms it's a valid Marg data folder
        if (p / "MDIS.DBF").exists() or (p / "mdis.dbf").exists():
            logger.info("Auto-detected Marg data path", extra={"path": path})
            return path
    return None