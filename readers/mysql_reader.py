"""
mysql_reader.py — Reads pharmacy sales from a local MySQL database.

WHICH SOFTWARES USE THIS:
    - GoFrugal (on-premise / older installations) — most common
    - Some older versions of Pharmacy Pro
    - Any locally-installed pharmacy software that uses MySQL

HOW TO FIND THE DATABASE:
    GoFrugal typically installs MySQL on localhost:3306.
    Database name is usually "gofrugal", "rpos", or "youstar".
    Default credentials: root / (blank or "gofrugal")

    The MySQL instance is often a bundled version shipped with GoFrugal —
    NOT the system MySQL. It runs on port 3306 (sometimes 3307 to avoid conflicts).

HOW TO READ:
    pip install mysql-connector-python

    We connect as read-only. We never write to the pharmacy's database.
    We read with LIMIT and ORDER BY to be efficient.

GOFRUGAL TABLE STRUCTURE (reverse-engineered from GoFrugal RPOS):
    Key tables:
    - `salesbill`         / `bill_master`    — sales header
    - `salesbilldetail`   / `bill_detail`    — line items
    - `customer`          / `party`          — customer master

    Field names vary between GoFrugal versions (5.x, 6.x, 7.x).
    We try all known variants.

SECURITY NOTE:
    We only ever do SELECT queries.
    The connection is read-only by design (we request read-only mode).
    We never store MySQL credentials in the cloud — they stay in config.json locally.
"""

import logging
import re
from datetime import date, datetime
from typing import Optional

logger = logging.getLogger(__name__)


# ── Known table/field names across GoFrugal versions ─────────────────────────

# (table_name, bill_no_col, date_col, cust_name_col, phone_col, cust_id_col)
MASTER_TABLE_VARIANTS = [
    # GoFrugal RPOS v6/v7
    ("salesbill",     "billno",      "billdate",  "customername", "mobile",    "customerid"),
    ("bill_master",   "bill_no",     "bill_date", "party_name",   "mobile_no", "party_id"),
    # GoFrugal YouStar
    ("youstar_sales", "invoice_no",  "inv_date",  "cust_name",    "phone",     "cust_id"),
    # Generic fallbacks
    ("sales",         "bill_no",     "date",      "customer",     "mobile",    "cust_id"),
    ("invoices",      "invoice_no",  "date",      "party",        "phone",     "party_id"),
]

# (table_name, bill_no_col, item_name_col, qty_col, rate_col)
DETAIL_TABLE_VARIANTS = [
    ("salesbilldetail", "billno",     "itemname",    "qty",      "rate"),
    ("bill_detail",     "bill_no",    "product_name","quantity", "mrp"),
    ("salesdetail",     "bill_no",    "item",        "qty",      "rate"),
    ("invoice_items",   "invoice_no", "item_name",   "qty",      "price"),
]

CUSTOMER_TABLE_VARIANTS = [
    ("customer", "customerid", "customername", "mobile"),
    ("party",    "party_id",   "party_name",   "mobile_no"),
    ("accounts", "id",         "name",         "phone"),
]


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
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _estimate_refill_days(qty: int) -> int:
    return max(7, min(90, qty * 10)) if qty > 0 else 30


# ── Connection ────────────────────────────────────────────────────────────────

def _get_connection(host: str, port: int, user: str, password: str, database: str):
    try:
        import mysql.connector
    except ImportError:
        raise ImportError(
            "mysql-connector-python not installed.\n"
            "Run: pip install mysql-connector-python"
        )

    return mysql.connector.connect(
        host=host,
        port=port,
        user=user,
        password=password,
        database=database,
        connection_timeout=10,
        autocommit=True,
    )


def _table_exists(cursor, table_name: str, database: str) -> bool:
    cursor.execute(
        "SELECT COUNT(*) FROM information_schema.tables "
        "WHERE table_schema=%s AND table_name=%s",
        (database, table_name)
    )
    return cursor.fetchone()[0] > 0


def _fetch_query(cursor, sql: str, params=()) -> list[dict]:
    cursor.execute(sql, params)
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


# ── Main reader ───────────────────────────────────────────────────────────────

def read_new_sales(
    data_path: str,
    since_date: date,
) -> list[dict]:
    """
    Read new pharmacy sales from a local MySQL database.

    Args:
        data_path: Connection string in format:
                   "host:port:database:user:password"
                   e.g. "localhost:3306:gofrugal:root:"
                   (empty password is fine)
        since_date: Only return bills from this date onward

    Returns:
        List of BillRecord dicts
    """
    # Parse connection string
    try:
        parts = data_path.split(":")
        host     = parts[0]
        port     = int(parts[1]) if len(parts) > 1 else 3306
        database = parts[2] if len(parts) > 2 else "gofrugal"
        user     = parts[3] if len(parts) > 3 else "root"
        password = parts[4] if len(parts) > 4 else ""
    except (IndexError, ValueError) as e:
        logger.error(
            "Invalid MySQL connection string. "
            "Expected format: host:port:database:user:password",
            extra={"data_path": data_path, "error": str(e)}
        )
        return []

    try:
        conn = _get_connection(host, port, user, password, database)
    except Exception as e:
        logger.error(
            "Cannot connect to MySQL",
            extra={"host": host, "port": port, "database": database, "error": str(e)}
        )
        return []

    cursor = conn.cursor()

    try:
        # ── Find master table ─────────────────────────────────────────────────
        master_config = None
        for variant in MASTER_TABLE_VARIANTS:
            table_name = variant[0]
            if _table_exists(cursor, table_name, database):
                master_config = variant
                logger.info("Found sales master table", extra={"table": table_name})
                break

        if not master_config:
            logger.error(
                "No recognisable sales table found in MySQL database",
                extra={"database": database}
            )
            return []

        master_tbl, bill_no_col, date_col, name_col, phone_col, cust_id_col = master_config

        # ── Find detail table ─────────────────────────────────────────────────
        detail_config = None
        for variant in DETAIL_TABLE_VARIANTS:
            if _table_exists(cursor, variant[0], database):
                detail_config = variant
                logger.info("Found sales detail table", extra={"table": variant[0]})
                break

        # ── Load customer master ──────────────────────────────────────────────
        cust_phone_map: dict[str, str] = {}
        for cust_variant in CUSTOMER_TABLE_VARIANTS:
            ctable, cid_col, cname_col, cphone_col = cust_variant
            if _table_exists(cursor, ctable, database):
                rows = _fetch_query(
                    cursor,
                    f"SELECT `{cid_col}`, `{cphone_col}` FROM `{ctable}`"
                )
                for row in rows:
                    cid   = str(row.get(cid_col, "")).strip()
                    phone = _normalize_phone(str(row.get(cphone_col, "")))
                    if cid and phone:
                        cust_phone_map[cid] = phone
                logger.info("Customer master loaded", extra={"count": len(cust_phone_map)})
                break

        # ── Load detail rows → group by bill_no ──────────────────────────────
        bill_medicines: dict[str, list] = {}
        if detail_config:
            dtable, d_bill_col, d_item_col, d_qty_col, d_rate_col = detail_config
            detail_rows = _fetch_query(
                cursor,
                f"SELECT `{d_bill_col}`, `{d_item_col}`, `{d_qty_col}`, `{d_rate_col}` "
                f"FROM `{dtable}`"
            )
            for row in detail_rows:
                bill_no   = str(row.get(d_bill_col, "")).strip()
                item_name = str(row.get(d_item_col, "")).strip()
                if not bill_no or not item_name:
                    continue

                qty_raw = row.get(d_qty_col)
                try:
                    qty = int(float(qty_raw)) if qty_raw is not None else 1
                except (ValueError, TypeError):
                    qty = 1

                rate_raw = row.get(d_rate_col)
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

        # ── Load master rows filtered by date ─────────────────────────────────
        master_rows = _fetch_query(
            cursor,
            f"SELECT * FROM `{master_tbl}` WHERE `{date_col}` >= %s ORDER BY `{date_col}` DESC",
            (since_date.isoformat(),)
        )

        results = []
        skipped = {"date": 0, "phone": 0, "no_meds": 0}

        for row in master_rows:
            bill_date = _parse_date(row.get(date_col))
            if not bill_date or bill_date < since_date:
                skipped["date"] += 1
                continue

            bill_no   = str(row.get(bill_no_col, "")).strip()
            cust_name = str(row.get(name_col, "")).strip()

            if not cust_name or cust_name.lower() in ("cash", "retail", "walk-in", "counter", ""):
                skipped["phone"] += 1
                continue

            phone = _normalize_phone(str(row.get(phone_col, "")))
            if not phone:
                cust_id = str(row.get(cust_id_col, "")).strip()
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
                "source":         "mysql_local",
                "raw_invoice_id": bill_no,
            })

        logger.info(
            "MySQL read complete",
            extra={"returned": len(results), "skipped": skipped}
        )
        return results

    finally:
        cursor.close()
        conn.close()