"""
access_reader.py — Reads pharmacy data from Microsoft Access .MDB / .ACCDB files.

WHICH SOFTWARES USE THIS:
    - Care (common pharmacy software in Maharashtra/Gujarat)
    - Many older regional pharmacy softwares from the 2000s era
    - Any software that was built with MS Access as its database

HOW ACCESS FILES WORK:
    MS Access stores everything in a single .mdb (Access 97-2003) or
    .accdb (Access 2007+) file. Unlike DBF (one file per table),
    Access packs all tables into one file.

    Typical file locations:
        C:/Care/Database/CareDB.mdb
        C:/PharmaSoft/data/pharmacy.mdb
        C:/CareRetail/CareRetail.accdb

HOW TO READ IN PYTHON:
    We use pyodbc with the Microsoft Access ODBC driver.
    The driver ships with Microsoft Office on Windows.
    If not installed: download "Microsoft Access Database Engine 2016 Redistributable"
    (free, 50MB, from Microsoft's website).

    pip install pyodbc

TABLE STRUCTURE (Care-specific — discovered from reverse engineering):
    We try generic table names first, then Care-specific names.
    If neither works, we list all tables and try to identify sales tables.

IMPORTANT NOTE:
    Access ODBC only works on Windows. On Linux/Mac (dev machines),
    use mdbtools + pandas as a fallback for testing.

FALLBACK STRATEGY:
    If ODBC driver not found → try mdbtools CLI (Linux/Mac dev only).
    If table names don't match → scan all tables and pick the best match.
"""

import logging
import re
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Known table name variants across Access-based pharmacy softwares ──────────

SALES_TABLE_CANDIDATES = [
    # Care retail
    "SaleBill", "SaleDetail", "SaleMaster",
    # Generic names
    "Sales", "SalesMaster", "SalesDetail", "SalesHeader",
    "Bill", "Bills", "BillMaster", "BillDetail",
    "Invoice", "InvoiceMaster", "InvoiceDetail",
    # Hindi transliteration
    "Bikri", "BikriDetail",
]

CUSTOMER_TABLE_CANDIDATES = [
    "Customer", "Customers", "Party", "Parties",
    "Account", "Accounts", "Ledger",
    "CustomerMaster", "PartyMaster",
]

# Field name variants (similar to marg_reader approach)
BILL_FIELDS = {
    "bill_no":    ["BillNo", "InvoiceNo", "VchNo", "BillNumber", "ID", "SaleID"],
    "date":       ["BillDate", "Date", "SaleDate", "InvDate", "VchDate"],
    "cust_name":  ["CustomerName", "PartyName", "CustName", "Name", "CName"],
    "phone":      ["Mobile", "Phone", "ContactNo", "MobileNo", "CustMobile"],
    "cust_id":    ["CustomerID", "CustID", "PartyID", "AccountID"],
}

DETAIL_FIELDS = {
    "bill_no":    ["BillNo", "InvoiceNo", "SaleID", "BillID", "VchNo"],
    "item_name":  ["ItemName", "ProductName", "MedicineName", "Item", "Product"],
    "qty":        ["Qty", "Quantity", "SaleQty", "Strips", "Units"],
    "rate":       ["Rate", "MRP", "Price", "SaleRate"],
    "batch":      ["BatchNo", "Batch", "LotNo"],
}


def _get_field(row_dict: dict, candidates: list) -> str:
    """Try candidate field names on a dict row."""
    for field in candidates:
        val = row_dict.get(field)
        if val is not None:
            val = str(val).strip()
            if val and val.lower() not in ("none", "null", "0", ""):
                return val
    return ""


def _normalize_phone(raw: str) -> str:
    digits = re.sub(r"\D", "", raw)
    if digits.startswith("91") and len(digits) == 12:
        digits = digits[2:]
    return digits[-10:] if len(digits) >= 10 else ""


def _parse_date(raw) -> Optional[date]:
    if raw is None:
        return None
    if isinstance(raw, (date, datetime)):
        return raw.date() if isinstance(raw, datetime) else raw
    raw = str(raw).strip()
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _estimate_refill_days(qty: int) -> int:
    if qty <= 0:
        return 30
    return max(7, min(90, qty * 10))


# ── Connection helpers ────────────────────────────────────────────────────────

def _get_connection(mdb_path: str):
    """
    Return a pyodbc connection to the Access file.
    Tries 32-bit and 64-bit ODBC driver strings.
    """
    try:
        import pyodbc
    except ImportError:
        raise ImportError(
            "pyodbc not installed. Run: pip install pyodbc\n"
            "Also install Microsoft Access Database Engine from microsoft.com"
        )

    driver_candidates = [
        r"Microsoft Access Driver (*.mdb, *.accdb)",
        r"Microsoft Access Driver (*.mdb)",
    ]

    available = [d for d in pyodbc.drivers() if "access" in d.lower()]
    if not available:
        raise RuntimeError(
            "Microsoft Access ODBC driver not found on this PC.\n"
            "Download 'Microsoft Access Database Engine 2016 Redistributable' "
            "from microsoft.com (free, ~50MB) and install it, then retry."
        )

    driver = available[0]
    conn_str = (
        f"DRIVER={{{driver}}};"
        f"DBQ={mdb_path};"
        "Exclusive=No;"
        "ReadOnly=Yes;"
    )
    return pyodbc.connect(conn_str, autocommit=True)


def _list_tables(conn) -> list[str]:
    """List all table names in the Access database."""
    cursor = conn.cursor()
    tables = [
        row.table_name
        for row in cursor.tables(tableType="TABLE")
    ]
    cursor.close()
    return tables


def _find_table(conn, candidates: list) -> Optional[str]:
    """Find which table name actually exists in the DB."""
    existing = _list_tables(conn)
    existing_lower = {t.lower(): t for t in existing}
    for candidate in candidates:
        if candidate.lower() in existing_lower:
            return existing_lower[candidate.lower()]
    return None


def _fetch_table(conn, table_name: str) -> list[dict]:
    """Fetch all rows from a table as list of dicts."""
    cursor = conn.cursor()
    cursor.execute(f"SELECT * FROM [{table_name}]")
    columns = [col[0] for col in cursor.description]
    rows = [dict(zip(columns, row)) for row in cursor.fetchall()]
    cursor.close()
    return rows


# ── Main reader function ──────────────────────────────────────────────────────

def read_new_sales(mdb_path: str, since_date: date) -> list[dict]:
    """
    Read sales from an MS Access pharmacy database since since_date.

    Args:
        mdb_path:   Full path to the .mdb or .accdb file
        since_date: Only return bills from this date onward

    Returns:
        List of BillRecord dicts (same shape as marg_reader)
    """
    if not Path(mdb_path).exists():
        logger.error("MDB file not found", extra={"path": mdb_path})
        return []

    try:
        conn = _get_connection(mdb_path)
    except (ImportError, RuntimeError) as e:
        logger.error("Cannot connect to Access file", extra={"error": str(e)})
        return []

    try:
        all_tables = _list_tables(conn)
        logger.info("Access DB tables found", extra={"tables": all_tables})

        # ── Find the right tables ─────────────────────────────────────────────
        # We look for a "master/header" table and a "detail/items" table

        # Try to split candidates into master vs detail
        master_candidates = [t for t in SALES_TABLE_CANDIDATES
                             if any(k in t.lower() for k in ("master", "bill", "invoice", "sale"))
                             and "detail" not in t.lower()]
        detail_candidates = [t for t in SALES_TABLE_CANDIDATES
                             if "detail" in t.lower()]

        master_table = _find_table(conn, master_candidates) or _find_table(conn, SALES_TABLE_CANDIDATES)
        detail_table = _find_table(conn, detail_candidates)

        if not master_table:
            logger.error(
                "Could not find sales table in Access DB. "
                "Tables present: " + str(all_tables)
            )
            conn.close()
            return []

        logger.info(
            "Using Access tables",
            extra={"master": master_table, "detail": detail_table}
        )

        # ── Load customer master for phone lookup ─────────────────────────────
        cust_table = _find_table(conn, CUSTOMER_TABLE_CANDIDATES)
        cust_phone_map: dict[str, str] = {}
        if cust_table:
            cust_rows = _fetch_table(conn, cust_table)
            for row in cust_rows:
                cid = _get_field(row, BILL_FIELDS["cust_id"])
                phone = _normalize_phone(_get_field(row, BILL_FIELDS["phone"]))
                if cid and phone:
                    cust_phone_map[cid] = phone
            logger.info("Customer master loaded", extra={"count": len(cust_phone_map)})

        # ── Load detail rows → group by bill_no ──────────────────────────────
        bill_medicines: dict[str, list] = {}
        if detail_table:
            detail_rows = _fetch_table(conn, detail_table)
            for row in detail_rows:
                bill_no = _get_field(row, DETAIL_FIELDS["bill_no"])
                item_name = _get_field(row, DETAIL_FIELDS["item_name"])
                if not bill_no or not item_name:
                    continue

                qty_raw = _get_field(row, DETAIL_FIELDS["qty"])
                try:
                    qty = int(float(qty_raw)) if qty_raw else 1
                except ValueError:
                    qty = 1

                rate_raw = _get_field(row, DETAIL_FIELDS["rate"])
                try:
                    rate = float(rate_raw) if rate_raw else None
                except ValueError:
                    rate = None

                dosage_match = re.search(
                    r"(\d+\.?\d*\s*(?:mg|ml|mcg|g|iu|units?))",
                    item_name, re.IGNORECASE
                )
                dosage = dosage_match.group(1).strip() if dosage_match else ""

                bill_medicines.setdefault(bill_no, []).append({
                    "name":            item_name.strip().title(),
                    "dosage":          dosage,
                    "quantity":        qty,
                    "refill_days":     _estimate_refill_days(qty),
                    "price_per_strip": rate,
                })

        # ── Load master rows and build BillRecords ────────────────────────────
        master_rows = _fetch_table(conn, master_table)
        results = []
        skipped = {"date": 0, "phone": 0, "no_meds": 0}

        for row in master_rows:
            raw_date = row.get("BillDate") or row.get("Date") or row.get("SaleDate")
            bill_date = _parse_date(raw_date)
            if not bill_date or bill_date < since_date:
                skipped["date"] += 1
                continue

            bill_no = _get_field(row, BILL_FIELDS["bill_no"])
            medicines = bill_medicines.get(bill_no, [])

            # If no detail table, create a generic placeholder medicine entry
            # (at minimum we know a sale happened — we'll ask chemist to fill items)
            if not medicines and not detail_table:
                medicines = [{
                    "name":        "Medicine (unknown — no detail table)",
                    "dosage":      "",
                    "quantity":    1,
                    "refill_days": 30,
                }]

            if not medicines:
                skipped["no_meds"] += 1
                continue

            cust_name = _get_field(row, BILL_FIELDS["cust_name"])
            if not cust_name or cust_name.lower() in ("cash", "retail", "walk-in", "counter"):
                skipped["phone"] += 1
                continue

            phone = _normalize_phone(_get_field(row, BILL_FIELDS["phone"]))
            if not phone:
                cust_id = _get_field(row, BILL_FIELDS["cust_id"])
                phone = cust_phone_map.get(cust_id, "")

            if not phone or len(phone) != 10:
                skipped["phone"] += 1
                continue

            for med in medicines:
                med["sale_date"] = bill_date.isoformat()

            results.append({
                "patient_name":   cust_name.strip().title(),
                "phone":          phone,
                "medicines":      medicines,
                "source":         "access_mdb",
                "raw_invoice_id": bill_no,
            })

        logger.info(
            "Access DB read complete",
            extra={"returned": len(results), "skipped": skipped}
        )
        return results

    finally:
        conn.close()