"""
sqlite_reader.py — Reads pharmacy sales from local SQLite database files.

WHICH SOFTWARES USE THIS:
    - Pharmacy Pro (popular in Tier 2/3 cities, syncs to Google Drive)
    - Some smaller regional apps
    - Any pharmacy software where you find a .db or .sqlite file

WHY SQLITE:
    Newer desktop apps (built post-2015) often use SQLite because:
    - No separate MySQL installation needed
    - Single file = easy backup (just copy the .db file)
    - Works offline with no server process

HOW TO FIND THE FILE:
    Common locations:
        C:/PharmacyPro/data/pharmacy.db
        ~/AppData/Local/PharmacyPro/pharmacy.db
        ~/Documents/PharmacyData/sales.sqlite
        C:/ProgramData/PharmSoft/db/main.db

HOW TO READ:
    Python's built-in sqlite3 module — NO external library needed.
    This is the simplest reader of the four.

TABLE STRUCTURE:
    SQLite schemas vary widely. We scan table names and column names
    dynamically rather than hardcoding, so this works across different apps.
"""

import logging
import re
import sqlite3
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Table name candidates ─────────────────────────────────────────────────────

MASTER_TABLE_CANDIDATES = [
    "sales", "sale", "bill", "bills", "invoice", "invoices",
    "sale_master", "bill_master", "invoice_master",
    "transactions", "orders",
]

DETAIL_TABLE_CANDIDATES = [
    "sale_items", "bill_items", "invoice_items", "sales_detail",
    "sale_detail", "order_items", "transaction_items",
    "items", "medicine_sales",
]

CUSTOMER_TABLE_CANDIDATES = [
    "customers", "customer", "patients", "patient",
    "party", "parties", "contacts", "accounts",
]

# Field keyword patterns (we match column names by keywords)
DATE_KEYWORDS    = ["date", "time", "created", "billed"]
BILLNO_KEYWORDS  = ["bill", "invoice", "vch", "ref", "id", "no", "num"]
NAME_KEYWORDS    = ["name", "customer", "patient", "party", "cust"]
PHONE_KEYWORDS   = ["mobile", "phone", "contact", "mob"]
ITEM_KEYWORDS    = ["item", "medicine", "product", "drug", "name"]
QTY_KEYWORDS     = ["qty", "quantity", "strips", "units", "amount"]
RATE_KEYWORDS    = ["rate", "mrp", "price", "cost"]


def _find_col(columns: list[str], keywords: list[str]) -> Optional[str]:
    """Find the first column whose name contains any of the keywords (case-insensitive)."""
    for kw in keywords:
        for col in columns:
            if kw.lower() in col.lower():
                return col
    return None


def _normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", str(raw))
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    return digits[-10:] if len(digits) >= 10 else ""


def _parse_date(raw) -> Optional[date]:
    if raw is None:
        return None
    if isinstance(raw, (date, datetime)):
        return raw.date() if isinstance(raw, datetime) else raw
    raw = str(raw).strip()
    # Handle Unix timestamp
    if raw.isdigit() and len(raw) >= 10:
        try:
            return datetime.fromtimestamp(int(raw[:10])).date()
        except (ValueError, OSError):
            pass
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%Y-%m-%d %H:%M:%S"):
        try:
            return datetime.strptime(raw[:19], fmt).date()
        except ValueError:
            continue
    return None


def _estimate_refill_days(qty: int) -> int:
    return max(7, min(90, qty * 10)) if qty > 0 else 30


def _get_tables(conn) -> list[str]:
    cursor = conn.cursor()
    cursor.execute("SELECT name FROM sqlite_master WHERE type='table'")
    return [row[0] for row in cursor.fetchall()]


def _get_columns(conn, table: str) -> list[str]:
    cursor = conn.cursor()
    cursor.execute(f"PRAGMA table_info([{table}])")
    return [row[1] for row in cursor.fetchall()]


def _find_table(conn, candidates: list[str]) -> Optional[str]:
    existing = {t.lower(): t for t in _get_tables(conn)}
    for candidate in candidates:
        if candidate.lower() in existing:
            return existing[candidate.lower()]
    return None


# ── Main reader ───────────────────────────────────────────────────────────────

def read_new_sales(db_path: str, since_date: date) -> list[dict]:
    """
    Read new pharmacy sales from a local SQLite database.

    Args:
        db_path:    Full path to the .db or .sqlite file
        since_date: Only return bills from this date onward

    Returns:
        List of BillRecord dicts
    """
    if not Path(db_path).exists():
        logger.error("SQLite file not found", extra={"path": db_path})
        return []

    try:
        conn = sqlite3.connect(db_path)
        conn.row_factory = sqlite3.Row  # makes rows dict-like
    except Exception as e:
        logger.error("Cannot open SQLite file", extra={"path": db_path, "error": str(e)})
        return []

    try:
        all_tables = _get_tables(conn)
        logger.info("SQLite tables found", extra={"tables": all_tables})

        # ── Find relevant tables ──────────────────────────────────────────────
        master_table = _find_table(conn, MASTER_TABLE_CANDIDATES)
        detail_table = _find_table(conn, DETAIL_TABLE_CANDIDATES)
        cust_table   = _find_table(conn, CUSTOMER_TABLE_CANDIDATES)

        if not master_table:
            logger.error("No recognisable sales table in SQLite DB", extra={"tables": all_tables})
            return []

        logger.info(
            "SQLite tables selected",
            extra={"master": master_table, "detail": detail_table, "customer": cust_table}
        )

        # ── Introspect column names dynamically ───────────────────────────────
        master_cols = _get_columns(conn, master_table)
        date_col    = _find_col(master_cols, DATE_KEYWORDS)
        billno_col  = _find_col(master_cols, BILLNO_KEYWORDS)
        name_col    = _find_col(master_cols, NAME_KEYWORDS)
        phone_col   = _find_col(master_cols, PHONE_KEYWORDS)
        custid_col  = _find_col(master_cols, ["customer_id", "cust_id", "party_id", "patient_id"])

        if not date_col:
            logger.error(
                "Cannot find date column in table",
                extra={"table": master_table, "columns": master_cols}
            )
            return []

        # ── Load customer phone map ───────────────────────────────────────────
        cust_phone_map: dict[str, str] = {}
        if cust_table:
            cust_cols = _get_columns(conn, cust_table)
            c_id_col  = _find_col(cust_cols, ["id", "customer_id", "cust_id", "party_id"])
            c_ph_col  = _find_col(cust_cols, PHONE_KEYWORDS)
            if c_id_col and c_ph_col:
                cursor = conn.cursor()
                cursor.execute(f"SELECT [{c_id_col}], [{c_ph_col}] FROM [{cust_table}]")
                for row in cursor.fetchall():
                    cid   = str(row[0] or "").strip()
                    phone = _normalize_phone(str(row[1] or ""))
                    if cid and phone:
                        cust_phone_map[cid] = phone
                logger.info("Customer master loaded", extra={"count": len(cust_phone_map)})

        # ── Load detail rows ──────────────────────────────────────────────────
        bill_medicines: dict[str, list] = {}
        if detail_table:
            dcols      = _get_columns(conn, detail_table)
            d_bill_col = _find_col(dcols, BILLNO_KEYWORDS)
            d_item_col = _find_col(dcols, ITEM_KEYWORDS)
            d_qty_col  = _find_col(dcols, QTY_KEYWORDS)
            d_rate_col = _find_col(dcols, RATE_KEYWORDS)

            if d_bill_col and d_item_col:
                cols_to_select = [c for c in [d_bill_col, d_item_col, d_qty_col, d_rate_col] if c]
                select_expr = ", ".join(f"[{c}]" for c in cols_to_select)
                cursor = conn.cursor()
                cursor.execute(f"SELECT {select_expr} FROM [{detail_table}]")
                for row in cursor.fetchall():
                    row_dict   = dict(zip(cols_to_select, row))
                    bill_no    = str(row_dict.get(d_bill_col, "")).strip()
                    item_name  = str(row_dict.get(d_item_col, "")).strip()
                    if not bill_no or not item_name:
                        continue

                    qty_raw = row_dict.get(d_qty_col)
                    try:
                        qty = int(float(qty_raw)) if qty_raw is not None else 1
                    except (ValueError, TypeError):
                        qty = 1

                    rate_raw = row_dict.get(d_rate_col)
                    try:
                        rate = float(rate_raw) if rate_raw is not None else None
                    except (ValueError, TypeError):
                        rate = None

                    dosage_match = re.search(
                        r"(\d+\.?\d*\s*(?:mg|ml|mcg|g|iu|units?))",
                        item_name, re.IGNORECASE
                    )
                    dosage = dosage_match.group(1).strip() if dosage_match else ""

                    bill_medicines.setdefault(bill_no, []).append({
                        "name":            item_name.title(),
                        "dosage":          dosage,
                        "quantity":        qty,
                        "refill_days":     _estimate_refill_days(qty),
                        "price_per_strip": rate,
                    })

        # ── Load master rows ──────────────────────────────────────────────────
        cursor = conn.cursor()
        cursor.execute(
            f"SELECT * FROM [{master_table}] WHERE [{date_col}] >= ? ORDER BY [{date_col}] DESC",
            (since_date.isoformat(),)
        )
        all_master_cols = [desc[0] for desc in cursor.description]
        master_rows = [dict(zip(all_master_cols, row)) for row in cursor.fetchall()]

        results = []
        skipped = {"date": 0, "phone": 0, "no_meds": 0}

        for row in master_rows:
            bill_date = _parse_date(row.get(date_col))
            if not bill_date or bill_date < since_date:
                skipped["date"] += 1
                continue

            bill_no   = str(row.get(billno_col, "") or "").strip()
            cust_name = str(row.get(name_col, "") or "").strip() if name_col else ""

            if not cust_name or cust_name.lower() in ("cash", "retail", "walk-in", "counter"):
                skipped["phone"] += 1
                continue

            phone = _normalize_phone(str(row.get(phone_col, "") or "")) if phone_col else ""
            if not phone and custid_col:
                cust_id = str(row.get(custid_col, "") or "").strip()
                phone = cust_phone_map.get(cust_id, "")

            if not phone or len(phone) != 10:
                skipped["phone"] += 1
                continue

            medicines = bill_medicines.get(bill_no, [])
            if not medicines:
                skipped["no_meds"] += 1
                continue

            for med in medicines:
                med["sale_date"] = bill_date.isoformat()

            results.append({
                "patient_name":   cust_name.title(),
                "phone":          phone,
                "medicines":      medicines,
                "source":         "sqlite_local",
                "raw_invoice_id": bill_no,
            })

        logger.info(
            "SQLite read complete",
            extra={"returned": len(results), "skipped": skipped}
        )
        return results

    finally:
        conn.close()