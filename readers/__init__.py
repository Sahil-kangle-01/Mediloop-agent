"""
readers/ — Universal billing software reader package.

Each reader module exports one function:
    read_new_sales(data_path: str, since_date: date) -> list[dict]

The returned list is always in the same BillRecord shape:
    {
        "patient_name":   str,
        "phone":          str,   # 10-digit
        "medicines":      list,
        "source":         str,   # "marg_dbf" | "access_mdb" | "mysql" | "sqlite" | "csv"
        "raw_invoice_id": str,
    }

agent.py only ever calls read_new_sales() — it doesn't care which reader is used.
detector.py picks the right reader and passes it to agent.py.
"""

from readers.marg_reader   import read_new_sales as marg_read
from readers.access_reader import read_new_sales as access_read
from readers.mysql_reader  import read_new_sales as mysql_read
from readers.sqlite_reader import read_new_sales as sqlite_read
from readers.csv_reader    import read_new_sales as csv_read

READER_MAP = {
    "marg":          marg_read,
    "winpharm":      marg_read,   # same DBF structure as Marg
    "visual_infosoft": marg_read, # same DBF structure
    "access":        access_read,
    "care":          access_read, # Care uses MDB
    "mysql":         mysql_read,
    "gofrugal_local": mysql_read,
    "sqlite":        sqlite_read,
    "pharmacy_pro":  sqlite_read,
    "csv":           csv_read,    # universal fallback
}


def get_reader(software_type: str):
    """Return the correct read_new_sales function for a software type."""
    reader = READER_MAP.get(software_type.lower())
    if not reader:
        raise ValueError(
            f"Unknown software type: '{software_type}'. "
            f"Valid types: {list(READER_MAP.keys())}"
        )
    return reader