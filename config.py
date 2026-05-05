"""
config.py — Persistent config for MediLoop Desktop Agent.

Config file (mediloop_agent_config.json) next to the .exe:
{
    "pharmacy_id":           "uuid-from-mediloop-dashboard",
    "api_token":             "48-char-token-from-mediloop-settings",
    "api_url":               "https://mediloop-production-1b1e.up.railway.app",
    "software_type":         "auto",
    "data_path":             "C:/MARG/DATA",
    "sync_interval_minutes": 30,
    "last_sync_date":        "2026-05-01",
    "agent_version":         "1.1.0"
}

FIX LOG vs v1.0.0:
  - api_token now required and validated before sync
  - api_url default fixed (no trailing slash)
  - setup_interactive() now has a GUI fallback (tkinter) for .exe use
  - is_configured() checks both pharmacy_id AND api_token
  - validate() added token length check
"""

import json
import logging
import sys
from datetime import date, timedelta
from pathlib import Path
from typing import Optional

logger = logging.getLogger(__name__)

AGENT_VERSION    = "1.1.0"
CONFIG_FILENAME  = "mediloop_agent_config.json"
DEFAULT_LOOKBACK = 7
DEFAULT_API_URL  = "https://mediloop-production-1b1e.up.railway.app"


def _config_path() -> Path:
    if getattr(sys, "frozen", False):
        base = Path(sys.executable).parent
    else:
        base = Path(__file__).parent
    return base / CONFIG_FILENAME


class AgentConfig:
    def __init__(self):
        self._path = _config_path()
        self._data: dict = {}
        self._load()

    def _load(self):
        if self._path.exists():
            try:
                with open(self._path, "r", encoding="utf-8") as f:
                    self._data = json.load(f)
                logger.info("Config loaded", extra={"path": str(self._path)})
            except Exception as e:
                logger.error("Config corrupted — starting fresh", extra={"error": str(e)})
                self._data = {}

    def save(self):
        try:
            with open(self._path, "w", encoding="utf-8") as f:
                json.dump(self._data, f, indent=2, ensure_ascii=False)
            logger.info("Config saved", extra={"path": str(self._path)})
        except Exception as e:
            logger.error("Failed to save config", extra={"error": str(e)})

    # ── Properties ────────────────────────────────────────────────────────────

    @property
    def pharmacy_id(self) -> Optional[str]:
        return self._data.get("pharmacy_id")
    @pharmacy_id.setter
    def pharmacy_id(self, v: str):
        self._data["pharmacy_id"] = v.strip()

    @property
    def api_token(self) -> Optional[str]:
        return self._data.get("api_token")
    @api_token.setter
    def api_token(self, v: str):
        self._data["api_token"] = v.strip()

    @property
    def api_url(self) -> str:
        return self._data.get("api_url", DEFAULT_API_URL).rstrip("/")
    @api_url.setter
    def api_url(self, v: str):
        self._data["api_url"] = v.strip().rstrip("/")

    @property
    def software_type(self) -> str:
        return self._data.get("software_type", "auto")
    @software_type.setter
    def software_type(self, v: str):
        self._data["software_type"] = v

    @property
    def data_path(self) -> Optional[str]:
        return self._data.get("data_path")
    @data_path.setter
    def data_path(self, v: str):
        self._data["data_path"] = v

    @property
    def sync_interval_minutes(self) -> int:
        return int(self._data.get("sync_interval_minutes", 30))
    @sync_interval_minutes.setter
    def sync_interval_minutes(self, v: int):
        self._data["sync_interval_minutes"] = v

    @property
    def last_sync_date(self) -> date:
        raw = self._data.get("last_sync_date")
        if raw:
            try:
                return date.fromisoformat(raw)
            except ValueError:
                pass
        return date.today() - timedelta(days=DEFAULT_LOOKBACK)

    def update_last_sync_date(self, sync_date: Optional[date] = None):
        self._data["last_sync_date"] = (sync_date or date.today()).isoformat()
        self._data["agent_version"]  = AGENT_VERSION
        self.save()

    # ── Validation ────────────────────────────────────────────────────────────

    def is_configured(self) -> bool:
        """True only if BOTH pharmacy_id AND api_token are set."""
        pid   = self._data.get("pharmacy_id", "").strip()
        token = self._data.get("api_token", "").strip()
        return bool(pid and token)

    def validate(self) -> list[str]:
        errors = []
        pid   = self._data.get("pharmacy_id", "").strip()
        token = self._data.get("api_token", "").strip()

        if not pid:
            errors.append("pharmacy_id is missing")
        elif len(pid) < 10:
            errors.append("pharmacy_id looks wrong — should be the UUID from MediLoop Settings")

        if not token:
            errors.append("api_token is missing — generate one in MediLoop Settings → Desktop Agent")
        elif len(token) < 20:
            errors.append("api_token looks wrong — should be the 48-char token from MediLoop Settings")

        sw = self.software_type
        dp = self.data_path
        if sw not in ("auto", "csv") and not dp:
            errors.append("data_path is missing")
        elif dp and sw in ("marg", "winpharm", "visual_infosoft") and not Path(dp).exists():
            errors.append(f"DBF folder not found: {dp}")
        elif dp and sw in ("access", "care", "sqlite", "pharmacy_pro") and not Path(dp).exists():
            errors.append(f"Database file not found: {dp}")

        return errors

    # ── Setup — GUI if possible, CLI fallback ─────────────────────────────────

    def setup_interactive(self):
        """
        Setup wizard. Uses tkinter GUI when running as .exe (no console).
        Falls back to CLI prompts when running from terminal.
        """
        try:
            self._setup_gui()
        except Exception as e:
            logger.warning("GUI setup failed, falling back to CLI", extra={"error": str(e)})
            self._setup_cli()

    def _setup_gui(self):
        import tkinter as tk
        from tkinter import ttk, messagebox

        root = tk.Tk()
        root.title("MediLoop Agent — Setup")
        root.geometry("480x400")
        root.resizable(False, False)
        root.configure(bg="#f8f9fa")

        # Try to set icon
        try:
            if getattr(sys, "frozen", False):
                icon_path = Path(sys.executable).parent / "icon.ico"
            else:
                icon_path = Path(__file__).parent / "icon.ico"
            if icon_path.exists():
                root.iconbitmap(str(icon_path))
        except Exception:
            pass

        # Header
        header = tk.Frame(root, bg="#1a5c38", height=60)
        header.pack(fill="x")
        tk.Label(
            header, text="MediLoop Desktop Agent Setup",
            bg="#1a5c38", fg="white",
            font=("Segoe UI", 14, "bold"),
            pady=16,
        ).pack()

        # Form
        form = tk.Frame(root, bg="#f8f9fa", padx=30, pady=20)
        form.pack(fill="both", expand=True)

        def field(label, default="", show=None):
            tk.Label(form, text=label, bg="#f8f9fa",
                     font=("Segoe UI", 9), anchor="w").pack(fill="x", pady=(8, 0))
            var = tk.StringVar(value=default)
            entry = tk.Entry(form, textvariable=var, font=("Segoe UI", 10),
                             relief="solid", bd=1, show=show)
            entry.pack(fill="x", ipady=4)
            return var

        tk.Label(
            form,
            text="Get your Pharmacy ID and API Token from:\nMediLoop Dashboard → Settings → Desktop Agent",
            bg="#f8f9fa", fg="#6c757d", font=("Segoe UI", 8),
            justify="left",
        ).pack(anchor="w", pady=(0, 8))

        pid_var   = field("Pharmacy ID", default=self.pharmacy_id or "")
        token_var = field("API Token", default=self.api_token or "", show="•")
        url_var   = field("Backend URL (don't change unless told to)", default=self.api_url)

        # Save button
        def on_save():
            pid   = pid_var.get().strip()
            token = token_var.get().strip()
            url   = url_var.get().strip().rstrip("/")

            if not pid or not token:
                messagebox.showerror(
                    "Missing fields",
                    "Pharmacy ID and API Token are required.\n\n"
                    "Get them from MediLoop Dashboard → Settings → Desktop Agent."
                )
                return

            self.pharmacy_id = pid
            self.api_token   = token
            self.api_url     = url or DEFAULT_API_URL
            if not self.software_type:
                self.software_type = "auto"
            self._data["agent_version"] = AGENT_VERSION
            self.save()

            messagebox.showinfo(
                "Setup Complete",
                "✓ Configuration saved!\n\n"
                "The agent will now auto-detect your billing software\n"
                "and start syncing every 30 minutes."
            )
            root.destroy()

        tk.Button(
            form, text="Save & Start Agent",
            bg="#1a5c38", fg="white",
            font=("Segoe UI", 11, "bold"),
            relief="flat", cursor="hand2",
            pady=8,
            command=on_save,
        ).pack(fill="x", pady=(16, 0))

        root.mainloop()

    def _setup_cli(self):
        print("\n" + "=" * 60)
        print("  MediLoop Desktop Agent — Setup")
        print("=" * 60)
        print("\nGet your credentials from:")
        print("  MediLoop Dashboard → Settings → Desktop Agent\n")

        self.pharmacy_id = input("  Pharmacy ID : ").strip()
        self.api_token   = input("  API Token   : ").strip()

        url = input(f"  Backend URL (Enter = {DEFAULT_API_URL}): ").strip()
        self.api_url = url or DEFAULT_API_URL

        print("\nScanning PC for billing software...")
        from detector import print_detection_report
        result = print_detection_report()

        use_detected = "y"
        if result["detected"]:
            use_detected = input("  Use detected software? (Y/n): ").strip().lower()

        if use_detected != "n" and result["detected"]:
            self.software_type = result["software"]
            self.data_path     = result["data_path"]
        else:
            print("\n  Types: marg | care | gofrugal_local | pharmacy_pro | csv | auto")
            self.software_type = input("  Software type (Enter = auto): ").strip().lower() or "auto"
            if self.software_type not in ("auto", "csv"):
                self.data_path = input("  Data path : ").strip().strip('"')

        interval = input("  Sync interval minutes (Enter = 30): ").strip()
        self.sync_interval_minutes = int(interval) if interval.isdigit() else 30

        self._data["agent_version"] = AGENT_VERSION
        self.save()

        print(f"\n✓ Setup complete! Software: {self.software_type}")
        errors = self.validate()
        if errors:
            for e in errors:
                print(f"  ⚠ {e}")
        else:
            print("✓ All settings validated.")
        print()


config = AgentConfig()
