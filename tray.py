"""
tray.py — Windows system tray icon for MediLoop Desktop Agent.

FIX LOG vs v1.0.0:
  - Dashboard URL fixed to open app URL not backend URL
  - Added "⚙ Re-run Setup" menu item so chemist can update token without CLI
  - Tooltip updates every 10s instead of 15s for more responsive feel
  - Added null check on icon_image before registering
"""

import logging
import os
import subprocess
import sys
import threading
from pathlib import Path
from typing import TYPE_CHECKING

if TYPE_CHECKING:
    from agent import SyncStatus
    from config import AgentConfig

logger = logging.getLogger(__name__)

APP_DASHBOARD_URL = "https://app.mediloop.in"   # frontend, not backend


def _make_icon_image():
    try:
        from PIL import Image, ImageDraw

        size   = 64
        img    = Image.new("RGBA", (size, size), (0, 0, 0, 0))
        draw   = ImageDraw.Draw(img)
        margin = 4
        color  = (34, 197, 94)
        outline= (22, 163, 74)

        draw.rounded_rectangle(
            [margin, margin * 2, size - margin, size - margin * 2],
            radius=18,
            fill=color,
            outline=outline,
            width=2,
        )

        try:
            from PIL import ImageFont
            font = ImageFont.truetype("arial.ttf", 28)
        except Exception:
            font = ImageFont.load_default()

        draw.text((size // 2, size // 2), "M", fill="white", font=font, anchor="mm")
        return img

    except Exception as e:
        logger.warning("Could not draw pill icon — using plain square", extra={"error": str(e)})
        try:
            from PIL import Image
            return Image.new("RGB", (64, 64), (34, 197, 94))
        except ImportError:
            return None


def _open_log_file():
    if getattr(sys, "frozen", False):
        log_path = Path(sys.executable).parent / "mediloop_agent.log"
    else:
        log_path = Path(__file__).parent / "mediloop_agent.log"

    if log_path.exists():
        try:
            os.startfile(str(log_path))
        except Exception:
            subprocess.Popen(["notepad.exe", str(log_path)])
    else:
        _show_balloon("MediLoop Agent", "No log file yet — sync hasn't run.")


def _open_config_file():
    if getattr(sys, "frozen", False):
        cfg_path = Path(sys.executable).parent / "mediloop_agent_config.json"
    else:
        cfg_path = Path(__file__).parent / "mediloop_agent_config.json"

    if cfg_path.exists():
        try:
            os.startfile(str(cfg_path))
        except Exception:
            subprocess.Popen(["notepad.exe", str(cfg_path)])
    else:
        _show_balloon("MediLoop Agent", "Config file not found — run setup first.")


def _open_dashboard():
    import webbrowser
    webbrowser.open(APP_DASHBOARD_URL)


def _show_balloon(title: str, message: str):
    try:
        import ctypes
        ctypes.windll.user32.MessageBoxW(0, message, title, 0x40)
    except Exception:
        pass


def run_tray(status: "SyncStatus", cfg: "AgentConfig"):
    """
    Start the system tray icon. Blocks until Exit is clicked.
    Must be called from the main thread on Windows.
    """
    try:
        import pystray
    except ImportError:
        logger.warning("pystray not installed — no tray icon")
        threading.Event().wait()
        return

    icon_image = _make_icon_image()
    if icon_image is None:
        logger.error("Cannot create tray icon — PIL not available")
        threading.Event().wait()
        return

    def _trigger_sync(icon, item):
        from agent import run_sync
        threading.Thread(target=run_sync, daemon=True, name="manual-sync").start()
        _show_balloon("MediLoop Agent", "Manual sync started...")

    def _rerun_setup(icon, item):
        """Re-open setup GUI — useful when token changes."""
        cfg.setup_interactive()
        _show_balloon("MediLoop Agent", "Setup saved. Next sync will use new credentials.")

    def _exit_agent(icon, item):
        logger.info("User exit from tray")
        icon.stop()
        sys.exit(0)

    def _update_tooltip(icon):
        import time
        while True:
            try:
                icon.title = status.tooltip()
            except Exception:
                pass
            time.sleep(10)

    menu = pystray.Menu(
        pystray.MenuItem("MediLoop Agent v1.1", None, enabled=False),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("🔄  Sync Now",          _trigger_sync),
        pystray.MenuItem("🌐  Open Dashboard",    lambda i, it: _open_dashboard()),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("📋  View Logs",          lambda i, it: _open_log_file()),
        pystray.MenuItem("⚙   Re-run Setup",      _rerun_setup),
        pystray.MenuItem("📄  View Config",        lambda i, it: _open_config_file()),
        pystray.Menu.SEPARATOR,
        pystray.MenuItem("❌  Exit",               _exit_agent),
    )

    icon = pystray.Icon(
        name="MediLoopAgent",
        icon=icon_image,
        title=status.tooltip(),
        menu=menu,
    )

    threading.Thread(
        target=_update_tooltip,
        args=(icon,),
        daemon=True,
        name="tooltip-updater",
    ).start()

    logger.info("Tray icon started")
    icon.run()
