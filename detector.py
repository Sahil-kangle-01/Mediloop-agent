"""
detector.py — Auto-detects which pharmacy billing software is installed.

PURPOSE:
    When the chemist runs MediLoopAgent.exe for the first time, we don't
    want to ask them "which software do you use?" — they often don't know
    the exact name. We scan the PC and figure it out automatically.

HOW IT WORKS:
    1. Scans common install paths for known indicator files
    2. For MySQL-based software, tries to connect to localhost:3306
    3. Returns detected software name, type, and data path
    4. Falls back to "csv" if nothing detected (universal fallback)

WHAT WE DETECT:
    ┌──────────────────┬───────────┬───────────────────────────────────┐
    │ Software         │ Type      │ Indicator                         │
    ├──────────────────┼───────────┼───────────────────────────────────┤
    │ Marg ERP         │ dbf       │ C:/MARG/DATA/MDIS.DBF             │
    │ Winpharm         │ dbf       │ C:/WinPharm/DATA/SALES.DBF        │
    │ Visual InfoSoft  │ dbf       │ C:/VIS/DATA/*.DBF                 │
    │ GoFrugal local   │ mysql     │ localhost:3306 database "gofrugal" │
    │ Care             │ access    │ C:/Care/Database/*.mdb            │
    │ Pharmacy Pro     │ sqlite    │ AppData/Local/PharmacyPro/*.db    │
    │ (anything else)  │ csv       │ fallback — drop folder            │
    └──────────────────┴───────────┴───────────────────────────────────┘

OUTPUT:
    {
        "software":  "marg",
        "type":      "dbf",          # determines which reader to use
        "data_path": "C:/MARG/DATA", # passed to reader's read_new_sales()
        "detected":  True,
        "confidence": "high",        # "high" | "medium" | "low"
        "notes":     "Found MDIS.DBF at C:/MARG/DATA"
    }
"""

import logging
import os
import socket
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)


# ── Software detection rules ──────────────────────────────────────────────────

DETECTION_RULES = [
    # ── DBF-based software ────────────────────────────────────────────────────
    {
        "software":   "marg",
        "type":       "dbf",
        "confidence": "high",
        "paths": [
            r"C:\MARG\DATA",
            r"C:\Marg ERP 9\DATA",
            r"C:\Marg ERP 10\DATA",
            r"C:\Marg ERP 11\DATA",
            r"C:\MARG9\DATA",
            r"C:\MARG10\DATA",
            r"C:\MARG11\DATA",
            r"D:\MARG\DATA",
            r"E:\MARG\DATA",
            r"C:\Program Files\MARG\DATA",
            r"C:\Program Files (x86)\MARG\DATA",
            r"C:\MARGDATA",
        ],
        "indicator_file": "MDIS.DBF",
        "notes": "Marg ERP detected via MDIS.DBF",
    },
    {
        "software":   "winpharm",
        "type":       "dbf",
        "confidence": "high",
        "paths": [
            r"C:\WinPharm\DATA",
            r"C:\WinPharm",
            r"C:\WINPHARM\DATA",
            r"D:\WinPharm\DATA",
        ],
        "indicator_file": "SALES.DBF",
        "notes": "WinPharm detected via SALES.DBF",
    },
    {
        "software":   "visual_infosoft",
        "type":       "dbf",
        "confidence": "medium",
        "paths": [
            r"C:\VIS\DATA",
            r"C:\VisualInfoSoft\DATA",
            r"C:\Visual InfoSoft\DATA",
        ],
        "indicator_file": None,   # any .DBF in folder
        "notes": "Visual InfoSoft detected via DBF files",
    },

    # ── Access-based software ─────────────────────────────────────────────────
    {
        "software":   "care",
        "type":       "access",
        "confidence": "high",
        "paths": [
            r"C:\Care\Database",
            r"C:\CareRetail\Database",
            r"C:\Care Retail\Database",
            r"C:\Care Software\Database",
            r"D:\Care\Database",
        ],
        "indicator_file": None,  # any .mdb or .accdb
        "file_extensions": [".mdb", ".accdb"],
        "notes": "Care software detected via .mdb database",
    },

    # ── SQLite-based software ─────────────────────────────────────────────────
    {
        "software":   "pharmacy_pro",
        "type":       "sqlite",
        "confidence": "high",
        "paths": [
            os.path.expandvars(r"%LOCALAPPDATA%\PharmacyPro"),
            os.path.expandvars(r"%APPDATA%\PharmacyPro"),
            r"C:\PharmacyPro",
            r"C:\Program Files\PharmacyPro",
        ],
        "indicator_file": "pharmacy.db",
        "notes": "Pharmacy Pro detected via pharmacy.db",
    },
    {
        "software":   "pharmeasy_pos",
        "type":       "sqlite",
        "confidence": "medium",
        "paths": [
            os.path.expandvars(r"%LOCALAPPDATA%\PharmEasy"),
            r"C:\PharmEasy\POS",
        ],
        "indicator_file": None,
        "file_extensions": [".db", ".sqlite"],
        "notes": "PharmEasy POS detected",
    },

    # ── MySQL-based software (detected by port, not file) ─────────────────────
    # GoFrugal is handled separately in detect_mysql_software()
]

# GoFrugal MySQL database name candidates
GOFRUGAL_DB_CANDIDATES = ["gofrugal", "rpos", "youstar", "pharmacy", "gofrugalrpos"]


# ── Detection functions ───────────────────────────────────────────────────────

def _find_file_in_folder(folder: Path, indicator_file: Optional[str],
                          extensions: Optional[list] = None) -> Optional[str]:
    """
    Find indicator_file in folder (case-insensitive).
    If indicator_file is None but extensions given, find first matching file.
    Returns full path string or None.
    """
    if not folder.exists():
        return None

    if indicator_file:
        for f in folder.iterdir():
            if f.name.lower() == indicator_file.lower():
                return str(f)
    elif extensions:
        for f in folder.iterdir():
            if f.suffix.lower() in extensions:
                return str(f)
    else:
        # Any DBF file = confirmed
        for f in folder.iterdir():
            if f.suffix.lower() == ".dbf":
                return str(folder)

    return None


def _check_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Return True if host:port is accepting connections."""
    try:
        sock = socket.create_connection((host, port), timeout=timeout)
        sock.close()
        return True
    except (socket.timeout, ConnectionRefusedError, OSError):
        return False


def detect_mysql_software() -> Optional[dict]:
    """
    Try to detect GoFrugal or other MySQL-based pharmacy software.
    Checks localhost:3306 and localhost:3307.
    """
    for port in (3306, 3307):
        if not _check_port_open("localhost", port):
            continue

        logger.info("MySQL port open", extra={"port": port})

        # Try to connect and check for GoFrugal database
        try:
            import mysql.connector
            for db_name in GOFRUGAL_DB_CANDIDATES:
                try:
                    conn = mysql.connector.connect(
                        host="localhost",
                        port=port,
                        user="root",
                        password="",       # GoFrugal default
                        database=db_name,
                        connection_timeout=3,
                    )
                    conn.close()
                    data_path = f"localhost:{port}:{db_name}:root:"
                    logger.info(
                        "GoFrugal MySQL detected",
                        extra={"database": db_name, "port": port}
                    )
                    return {
                        "software":   "gofrugal_local",
                        "type":       "mysql",
                        "data_path":  data_path,
                        "detected":   True,
                        "confidence": "high",
                        "notes":      f"GoFrugal MySQL on localhost:{port}, database '{db_name}'",
                    }
                except Exception:
                    continue
        except ImportError:
            # mysql-connector not installed — still report MySQL is present
            data_path = f"localhost:{port}:pharmacy:root:"
            return {
                "software":   "mysql",
                "type":       "mysql",
                "data_path":  data_path,
                "detected":   True,
                "confidence": "medium",
                "notes":      (
                    f"MySQL detected on localhost:{port} but could not verify database name. "
                    "Install mysql-connector-python and re-run detection."
                ),
            }

    return None


def detect_software() -> dict:
    """
    Main detection function. Scans the PC for installed pharmacy software.

    Returns:
        {
            "software":   str,   # e.g. "marg", "care", "gofrugal_local", "csv"
            "type":       str,   # "dbf" | "access" | "mysql" | "sqlite" | "csv"
            "data_path":  str,   # passed to the reader
            "detected":   bool,
            "confidence": str,   # "high" | "medium" | "low"
            "notes":      str,
        }
    """
    logger.info("Starting software detection scan")
    detections = []

    # ── File-based detection ──────────────────────────────────────────────────
    for rule in DETECTION_RULES:
        for path_str in rule["paths"]:
            folder = Path(path_str)
            indicator = rule.get("indicator_file")
            extensions = rule.get("file_extensions")

            found_path = _find_file_in_folder(folder, indicator, extensions)

            if found_path:
                # For Access/SQLite, data_path is the file; for DBF it's the folder
                if rule["type"] == "dbf":
                    data_path = str(folder)
                else:
                    data_path = found_path

                result = {
                    "software":   rule["software"],
                    "type":       rule["type"],
                    "data_path":  data_path,
                    "detected":   True,
                    "confidence": rule["confidence"],
                    "notes":      rule["notes"] + f" at {data_path}",
                }
                detections.append(result)
                logger.info("Software detected", extra=result)
                break  # Found this software — stop checking other paths for it

    # ── MySQL detection ───────────────────────────────────────────────────────
    mysql_result = detect_mysql_software()
    if mysql_result:
        detections.append(mysql_result)

    # ── Return best result ────────────────────────────────────────────────────
    if not detections:
        logger.info("No pharmacy software auto-detected — will use CSV drop folder")
        return {
            "software":   "csv",
            "type":       "csv",
            "data_path":  _default_csv_folder(),
            "detected":   False,
            "confidence": "low",
            "notes":      (
                "No billing software detected automatically. "
                "Using CSV drop folder. Ask chemist to export bills to this folder."
            ),
        }

    # Prioritise by confidence: high > medium > low
    priority = {"high": 0, "medium": 1, "low": 2}
    detections.sort(key=lambda d: priority.get(d["confidence"], 3))
    best = detections[0]

    if len(detections) > 1:
        logger.info(
            "Multiple software detected — using best match",
            extra={"chosen": best["software"], "all": [d["software"] for d in detections]}
        )

    return best


def _default_csv_folder() -> str:
    """Return the default CSV drop folder path."""
    desktop = Path(os.path.expandvars(r"%USERPROFILE%\Desktop"))
    return str(desktop / "MediLoopExports")


def print_detection_report():
    """Print a human-readable detection report for the setup wizard."""
    result = detect_software()
    print()
    if result["detected"]:
        print(f"  ✓ Detected: {result['software'].upper()} ({result['type'].upper()} format)")
        print(f"  ✓ Data path: {result['data_path']}")
        print(f"  ✓ Confidence: {result['confidence']}")
    else:
        print("  ⚠ No pharmacy software detected automatically.")
        print(f"  → Will use CSV drop folder: {result['data_path']}")
        print("  → Ask chemist to export bills here after each day.")
    print(f"  ℹ {result['notes']}")
    print()
    return result