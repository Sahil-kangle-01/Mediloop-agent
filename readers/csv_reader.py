"""
csv_reader.py — Universal CSV/Excel fallback reader.

WHEN THIS IS USED:
    When no direct database access is possible — e.g.:
    - Software doesn't use DBF/MySQL/SQLite/Access
    - Proprietary binary format (worst case)
    - Chemist uses a basic billing app that only exports CSV/Excel
    - Any software not covered by other readers

HOW IT WORKS:
    1. Watches a configured "drop folder" (e.g. C:/MediLoopExports/)
    2. Chemist manually exports from their billing software to this folder
       (most pharmacy software has File → Export or Reports → Export to CSV)
    3. Agent picks up new files, parses them, pushes to API
    4. Moves processed files to a "done/" subfolder to avoid reprocessing

WHAT WE ACCEPT:
    - .csv files (comma or semicolon separated)
    - .xlsx / .xls files (Excel)
    - Column names are flexible — matched by keyword (same as GPTCSVAdapter server-side)

COLUMN MATCHING (tries these in order):
    patient_name:  "patient name", "customer", "party", "name", "cust name"
    phone:         "mobile", "phone", "contact", "mob"
    medicine:      "medicine", "item", "product", "drug", "description"
    quantity:      "qty", "quantity", "strips"
    date:          "date", "sale date", "bill date"
    refill_days:   "refill", "days", "duration"

SETUP INSTRUCTION FOR CHEMIST:
    Tell them: "After making bills, go to Reports → Export and save the file
    to the MediLoopExports folder on your Desktop. Agent will pick it up automatically."
"""

import logging
import re
import shutil
from datetime import date, datetime
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Column matching keywords ──────────────────────────────────────────────────

COLUMN_MAP = {
    "patient_name": ["patient name", "customer name", "party name", "cust name",
                     "customer", "patient", "party", "name", "cname"],
    "phone":        ["mobile no", "phone no", "contact no", "mobile number",
                     "mobile", "phone", "contact", "mob", "ph"],
    "medicine":     ["medicine name", "item name", "product name", "drug name",
                     "medicine", "item", "product", "drug", "description", "iname"],
    "quantity":     ["quantity", "qty", "strips", "units", "sale qty"],
    "date":         ["sale date", "bill date", "invoice date", "date", "bdate"],
    "refill_days":  ["refill days", "refill", "days", "duration", "supply days"],
    "dosage":       ["dosage", "dose", "strength", "pack size"],
    "price":        ["rate", "mrp", "price", "amount", "cost"],
}


def _match_columns(headers: list[str]) -> dict[str, Optional[str]]:
    """
    Given a list of CSV headers, return a dict mapping our field names
    to the actual column name in the file. None if not found.
    """
    headers_lower = {h.lower().strip(): h for h in headers}
    matched: dict[str, Optional[str]] = {}

    for field, aliases in COLUMN_MAP.items():
        found = None
        for alias in aliases:
            if alias in headers_lower:
                found = headers_lower[alias]
                break
        # Fuzzy fallback: check if any header *contains* any alias keyword
        if not found:
            for alias in aliases:
                for h_lower, h_orig in headers_lower.items():
                    if alias in h_lower:
                        found = h_orig
                        break
                if found:
                    break
        matched[field] = found

    return matched


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
    for fmt in ("%Y-%m-%d", "%d/%m/%Y", "%d-%m-%Y", "%d/%m/%y", "%m/%d/%Y"):
        try:
            return datetime.strptime(raw, fmt).date()
        except ValueError:
            continue
    return None


def _estimate_refill_days(qty: int) -> int:
    return max(7, min(90, qty * 10)) if qty > 0 else 30


def _read_csv_file(file_path: Path) -> tuple[list[str], list[list[str]]]:
    """Read a CSV file. Auto-detect delimiter (comma or semicolon)."""
    import csv

    # Try UTF-8 first, fall back to latin-1 (Indian software)
    for encoding in ("utf-8-sig", "latin-1", "cp1252"):
        try:
            content = file_path.read_text(encoding=encoding)
            break
        except UnicodeDecodeError:
            continue
    else:
        raise ValueError(f"Cannot decode file: {file_path.name}")

    # Detect delimiter
    first_line = content.split("\n")[0]
    delimiter = ";" if first_line.count(";") > first_line.count(",") else ","

    reader = csv.reader(content.splitlines(), delimiter=delimiter)
    rows = list(reader)

    if len(rows) < 2:
        raise ValueError("File has fewer than 2 rows (no data)")

    headers = [h.strip() for h in rows[0]]
    data_rows = [[cell.strip() for cell in row] for row in rows[1:] if any(row)]

    return headers, data_rows


def _read_excel_file(file_path: Path) -> tuple[list[str], list[list[str]]]:
    """Read .xlsx or .xls file using openpyxl or xlrd."""
    try:
        import openpyxl
        wb = openpyxl.load_workbook(str(file_path), read_only=True, data_only=True)
        ws = wb.active
        rows = [[str(cell.value or "").strip() for cell in row] for row in ws.iter_rows()]
        wb.close()
    except ImportError:
        try:
            import xlrd
            wb = xlrd.open_workbook(str(file_path))
            ws = wb.sheet_by_index(0)
            rows = [[str(ws.cell_value(r, c)).strip() for c in range(ws.ncols)]
                    for r in range(ws.nrows)]
        except ImportError:
            raise ImportError(
                "Install openpyxl to read .xlsx files: pip install openpyxl\n"
                "Or xlrd for .xls files: pip install xlrd"
            )

    if not rows:
        raise ValueError("Excel file is empty")

    headers = rows[0]
    data_rows = [row for row in rows[1:] if any(row)]
    return headers, data_rows


def _parse_rows_to_records(
    headers: list[str],
    rows: list[list[str]],
    since_date: date,
    source_file: str,
) -> list[dict]:
    """Convert raw CSV/Excel rows to BillRecord dicts."""
    col_map = _match_columns(headers)

    logger.info(
        "Column mapping result",
        extra={"file": source_file, "mapping": col_map}
    )

    required = ["patient_name", "phone", "medicine"]
    missing  = [f for f in required if col_map[f] is None]
    if missing:
        logger.warning(
            "Missing required columns — file may not be parseable",
            extra={"missing": missing, "headers": headers}
        )

    # Group rows by patient + bill (patient_name + phone + date)
    # because CSV files are usually one row per medicine
    patient_bills: dict[str, dict] = {}

    for row in rows:
        row_dict = dict(zip(headers, row))

        # Patient name
        name_col = col_map["patient_name"]
        name = row_dict.get(name_col, "").strip().title() if name_col else ""
        if not name or name.lower() in ("cash", "retail", "walk-in", "counter"):
            continue

        # Phone
        phone_col = col_map["phone"]
        phone = _normalize_phone(row_dict.get(phone_col, "") if phone_col else "")
        if not phone or len(phone) != 10:
            continue

        # Date
        date_col = col_map["date"]
        raw_date = row_dict.get(date_col, "") if date_col else ""
        bill_date = _parse_date(raw_date) or date.today()
        if bill_date < since_date:
            continue

        # Medicine
        med_col = col_map["medicine"]
        med_name = row_dict.get(med_col, "").strip().title() if med_col else ""
        if not med_name:
            continue

        # Quantity
        qty_col = col_map["quantity"]
        qty_raw = row_dict.get(qty_col, "1") if qty_col else "1"
        try:
            qty = int(float(qty_raw)) if qty_raw else 1
        except (ValueError, TypeError):
            qty = 1

        # Dosage
        dos_col = col_map["dosage"]
        dosage = row_dict.get(dos_col, "").strip() if dos_col else ""
        if not dosage:
            m = re.search(r"(\d+\.?\d*\s*(?:mg|ml|mcg|g|iu|units?))", med_name, re.IGNORECASE)
            dosage = m.group(1).strip() if m else ""

        # Refill days
        ref_col = col_map["refill_days"]
        ref_raw = row_dict.get(ref_col, "") if ref_col else ""
        try:
            refill_days = int(float(ref_raw)) if ref_raw else _estimate_refill_days(qty)
        except (ValueError, TypeError):
            refill_days = _estimate_refill_days(qty)

        # Price
        price_col = col_map["price"]
        price_raw = row_dict.get(price_col, "") if price_col else ""
        try:
            price = float(price_raw) if price_raw else None
        except (ValueError, TypeError):
            price = None

        # Group by phone (deduplicate same patient across rows)
        key = phone
        if key not in patient_bills:
            patient_bills[key] = {
                "patient_name":   name,
                "phone":          phone,
                "medicines":      [],
                "source":         "csv_export",
                "raw_invoice_id": f"csv_{bill_date.isoformat()}_{phone[-4:]}",
            }

        patient_bills[key]["medicines"].append({
            "name":            med_name,
            "dosage":          dosage,
            "quantity":        qty,
            "refill_days":     refill_days,
            "price_per_strip": price,
            "sale_date":       bill_date.isoformat(),
        })

    return list(patient_bills.values())


# ── Main reader ───────────────────────────────────────────────────────────────

def read_new_sales(drop_folder: str, since_date: date) -> list[dict]:
    """
    Scan the drop folder for new CSV/Excel files and parse them.

    Args:
        drop_folder: Path to the folder the chemist drops exports into.
                     e.g. "C:/Users/Chemist/Desktop/MediLoopExports"
        since_date:  Only return records from this date onward.

    Returns:
        List of BillRecord dicts
    """
    folder = Path(drop_folder)
    if not folder.exists():
        try:
            folder.mkdir(parents=True, exist_ok=True)
            logger.info("Created drop folder", extra={"path": str(folder)})
        except Exception as e:
            logger.error("Cannot create drop folder", extra={"path": str(folder), "error": str(e)})
            return []

    done_folder = folder / "done"
    done_folder.mkdir(exist_ok=True)

    # Find all CSV and Excel files in the drop folder (not in done/)
    files = (
        list(folder.glob("*.csv")) +
        list(folder.glob("*.xlsx")) +
        list(folder.glob("*.xls"))
    )

    if not files:
        logger.info("No new files in drop folder", extra={"path": str(folder)})
        return []

    all_records: list[dict] = []

    for file_path in files:
        logger.info("Processing export file", extra={"file": file_path.name})
        try:
            if file_path.suffix.lower() == ".csv":
                headers, rows = _read_csv_file(file_path)
            else:
                headers, rows = _read_excel_file(file_path)

            records = _parse_rows_to_records(headers, rows, since_date, file_path.name)

            logger.info(
                "File parsed",
                extra={"file": file_path.name, "records": len(records)}
            )

            all_records.extend(records)

            # Move to done/ folder so we don't reprocess
            dest = done_folder / file_path.name
            if dest.exists():
                # Add timestamp to avoid collision
                ts = datetime.now().strftime("%Y%m%d_%H%M%S")
                dest = done_folder / f"{file_path.stem}_{ts}{file_path.suffix}"
            shutil.move(str(file_path), str(dest))
            logger.info("Moved to done folder", extra={"dest": str(dest)})

        except Exception as e:
            logger.error(
                "Failed to process file — skipping",
                extra={"file": file_path.name, "error": str(e)}
            )
            # Don't move file — let chemist retry after fixing the issue

    logger.info("CSV drop folder scan complete", extra={"total_records": len(all_records)})
    return all_records