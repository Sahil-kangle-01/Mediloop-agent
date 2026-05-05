"""
agent.py — MediLoop Desktop Sync Agent.

Run:        python agent.py
Setup:      python agent.py --setup
Test sync:  python agent.py --sync-once
No tray:    python agent.py --no-tray
Build exe:  pyinstaller MediLoopAgent.spec

FIX LOG vs v1.0.0:
  - push_to_mediloop() URL fixed — was missing /api/v1 prefix on CSV endpoint
  - Auth header now sends the permanent agent_token (not a JWT)
  - 401 error message now tells chemist exactly what to do
  - is_configured() now checks both pharmacy_id AND api_token before starting
  - --setup always triggers GUI setup wizard first
"""

import csv
import io
import logging
import sys
import time
import threading
import argparse
from datetime import date, datetime
from pathlib import Path

import requests
import schedule


def _setup_logging():
    if getattr(sys, "frozen", False):
        log_dir = Path(sys.executable).parent
    else:
        log_dir = Path(__file__).parent
    log_file = log_dir / "mediloop_agent.log"
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s  %(levelname)-8s  %(name)s  %(message)s",
        handlers=[
            logging.FileHandler(str(log_file), encoding="utf-8"),
            logging.StreamHandler(sys.stdout),
        ],
    )
    return log_file


log_file = _setup_logging()
logger   = logging.getLogger("mediloop.agent")

from config import config


# ── Sync status (shared with tray icon) ──────────────────────────────────────

class SyncStatus:
    def __init__(self):
        self.last_sync_time   = "Never"
        self.last_sync_result = "Waiting for first sync..."
        self.syncing          = False
        self.total_syncs      = 0
        self._lock            = threading.Lock()

    def update(self, result: str):
        with self._lock:
            self.last_sync_time   = datetime.now().strftime("%d %b %H:%M")
            self.last_sync_result = result
            self.syncing          = False
            self.total_syncs     += 1

    def tooltip(self) -> str:
        return (
            f"MediLoop Agent\n"
            f"Last sync: {self.last_sync_time}\n"
            f"{self.last_sync_result}"
        )


status = SyncStatus()


# ── Reader selection ──────────────────────────────────────────────────────────

def _resolve_reader():
    sw = config.software_type

    if sw == "auto":
        from detector import detect_software
        result = detect_software()
        config.software_type = result["software"]
        config.data_path     = result["data_path"]
        config.save()
        logger.info("Auto-detected software", extra={
            "software":  result["software"],
            "data_path": result["data_path"],
        })
        sw = config.software_type

    from readers import get_reader
    reader_fn = get_reader(sw)
    return reader_fn, config.data_path


# ── Convert BillRecords → CSV bytes ──────────────────────────────────────────

def records_to_csv_bytes(bill_records: list[dict]) -> bytes:
    output = io.StringIO()
    writer = csv.writer(output)
    writer.writerow([
        "patient_name", "phone", "medicine_name", "dosage",
        "quantity", "sale_date", "refill_days", "price",
    ])
    for record in bill_records:
        for med in record.get("medicines", []):
            writer.writerow([
                record.get("patient_name", ""),
                record.get("phone", ""),
                med.get("name", ""),
                med.get("dosage", ""),
                med.get("quantity", ""),
                med.get("sale_date", date.today().isoformat()),
                med.get("refill_days", 30),
                med.get("price_per_strip", ""),
            ])
    return output.getvalue().encode("utf-8")


# ── Push to MediLoop API ──────────────────────────────────────────────────────

def push_to_mediloop(bill_records: list[dict]) -> dict:
    csv_bytes = records_to_csv_bytes(bill_records)

    # FIXED: full path is /api/v1/integrations/import/csv
    url = f"{config.api_url}/api/v1/integrations/import/csv"

    headers = {
        "pharmacy-id":   config.pharmacy_id,
        "Authorization": f"Bearer {config.api_token}",
    }

    logger.info("Pushing to API", extra={
        "url":     url,
        "records": len(bill_records),
        "pharmacy_id": config.pharmacy_id,
    })

    resp = requests.post(
        url,
        headers=headers,
        files={"file": ("mediloop_sync.csv", csv_bytes, "text/csv")},
        timeout=30,
    )

    if resp.status_code == 200:
        return resp.json()

    elif resp.status_code == 401:
        raise PermissionError(
            "API token rejected (401). "
            "Go to MediLoop Dashboard → Settings → Desktop Agent → Regenerate Token, "
            "then re-run setup."
        )

    elif resp.status_code == 404:
        raise requests.RequestException(
            f"API endpoint not found (404). "
            f"URL tried: {url}. "
            f"Check that your backend is deployed and the URL in config is correct."
        )

    else:
        raise requests.RequestException(
            f"API returned {resp.status_code}: {resp.text[:300]}"
        )


# ── Core sync cycle ───────────────────────────────────────────────────────────

def run_sync():
    if status.syncing:
        logger.info("Sync already in progress — skipping")
        return

    status.syncing = True
    logger.info("Sync cycle starting", extra={
        "software":   config.software_type,
        "since_date": config.last_sync_date.isoformat(),
        "pharmacy_id": config.pharmacy_id,
    })

    try:
        reader_fn, data_path = _resolve_reader()
        bill_records = reader_fn(data_path, config.last_sync_date)

        if not bill_records:
            logger.info("No new sales since last sync")
            status.update("No new sales ✓")
            config.update_last_sync_date()
            return

        result = push_to_mediloop(bill_records)

        pc     = result.get("patients_created", 0)
        mc     = result.get("medicines_created", 0)
        errors = result.get("errors", [])

        config.update_last_sync_date()

        msg = f"✓ {pc} patients, {mc} medicines synced"
        if errors:
            msg = f"⚠ Synced with {len(errors)} errors"

        logger.info("Sync OK", extra={
            "patients":  pc,
            "medicines": mc,
            "errors":    len(errors),
        })
        status.update(msg)

    except PermissionError as e:
        logger.error("Auth error", extra={"error": str(e)})
        status.update("❌ Auth error — re-run setup")

    except requests.RequestException as e:
        logger.error("Network/API error", extra={"error": str(e)})
        status.update(f"⚠ {str(e)[:60]} — retry in {config.sync_interval_minutes}m")

    except Exception as e:
        logger.exception("Unexpected sync error")
        status.update(f"❌ {str(e)[:60]}")
        status.syncing = False


# ── Scheduler ─────────────────────────────────────────────────────────────────

def start_scheduler():
    logger.info("Scheduler starting", extra={"interval_min": config.sync_interval_minutes})
    run_sync()
    schedule.every(config.sync_interval_minutes).minutes.do(run_sync)
    while True:
        schedule.run_pending()
        time.sleep(30)


# ── Entry point ───────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="MediLoop Desktop Agent")
    parser.add_argument("--setup",     action="store_true", help="Run setup wizard")
    parser.add_argument("--sync-once", action="store_true", help="Sync once and exit (for testing)")
    parser.add_argument("--no-tray",   action="store_true", help="Run without system tray icon")
    args = parser.parse_args()

    # Always show setup if explicitly requested OR if not configured
    if args.setup or not config.is_configured():
        config.setup_interactive()
        if args.setup:
            print("Setup complete. Run the agent again to start syncing.")
            sys.exit(0)

    # Re-check after setup
    if not config.is_configured():
        print("\n❌ Setup incomplete. Both Pharmacy ID and API Token are required.")
        print("   Run with --setup to configure.")
        sys.exit(1)

    # Validate config
    errors = config.validate()
    if errors and config.software_type not in ("auto", "csv"):
        print("\n❌ Config errors:")
        for e in errors:
            print(f"  • {e}")
        print("\nRun with --setup to fix these.")
        sys.exit(1)

    logger.info("Agent starting", extra={
        "pharmacy_id": config.pharmacy_id,
        "software":    config.software_type,
        "data_path":   config.data_path,
        "last_sync":   config.last_sync_date.isoformat(),
        "api_url":     config.api_url,
        "version":     "1.1.0",
    })

    # Single sync test mode
    if args.sync_once:
        run_sync()
        print(f"\nResult: {status.last_sync_result}")
        sys.exit(0 if "✓" in status.last_sync_result else 1)

    # Full daemon mode
    if args.no_tray:
        start_scheduler()
    else:
        t = threading.Thread(target=start_scheduler, daemon=True, name="scheduler")
        t.start()
        try:
            from tray import run_tray
            run_tray(status, config)
        except ImportError:
            logger.warning("pystray not available — running without tray icon")
            print(f"Running without tray. Log: {log_file}")
            t.join()


if __name__ == "__main__":
    main()
