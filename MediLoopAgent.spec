# -*- mode: python ; coding: utf-8 -*-
#
# MediLoopAgent.spec — PyInstaller build spec
#
# Build command (run from repo root):
#   pyinstaller MediLoopAgent.spec
#
# Output: dist/MediLoopAgent.exe
#
# FIX LOG vs v1.0.0:
#   - Added hiddenimports for tkinter (GUI setup wizard)
#   - Added hiddenimports for all reader libraries
#   - Added icon.ico to datas so it's available at runtime for tray + setup GUI
#   - console=False is correct — no black window on launch
#   - uac_admin=False (don't ask for admin — we only read files, not write to system)

a = Analysis(
    ['agent.py'],
    pathex=[],
    binaries=[],
    datas=[
        ('readers', 'readers'),   # include all reader modules
        ('icon.ico', '.'),        # include icon next to exe for setup GUI
    ],
    hiddenimports=[
        # tkinter — needed for GUI setup wizard
        'tkinter',
        'tkinter.ttk',
        'tkinter.messagebox',

        # schedule library
        'schedule',

        # DBF reader (Marg, WinPharm)
        'dbfread',

        # MySQL reader (GoFrugal)
        'mysql.connector',
        'mysql.connector.locales',
        'mysql.connector.locales.eng',
        'mysql.connector.plugins',

        # Access reader
        'pyodbc',

        # Excel reader
        'openpyxl',
        'xlrd',

        # Tray icon
        'pystray',
        'PIL',
        'PIL.Image',
        'PIL.ImageDraw',
        'PIL.ImageFont',

        # Standard lib that PyInstaller sometimes misses
        'csv',
        'io',
        'json',
        'pathlib',
        'threading',
        'socket',
        'shutil',
        're',
    ],
    hookspath=[],
    hooksconfig={},
    runtime_hooks=[],
    excludes=[
        # Remove things we definitely don't need to keep .exe smaller
        'matplotlib',
        'numpy',
        'pandas',
        'scipy',
        'pytest',
        'setuptools',
    ],
    noarchive=False,
    optimize=0,
)

pyz = PYZ(a.pure)

exe = EXE(
    pyz,
    a.scripts,
    a.binaries,
    a.datas,
    [],
    name='MediLoopAgent',
    debug=False,
    bootloader_ignore_signals=False,
    strip=False,
    upx=True,
    upx_exclude=[],
    runtime_tmpdir=None,
    console=False,           # no console window
    disable_windowed_traceback=False,
    argv_emulation=False,
    target_arch=None,
    codesign_identity=None,
    entitlements_file=None,
    icon=['icon.ico'],
    uac_admin=False,         # don't request admin — read-only file access
)
