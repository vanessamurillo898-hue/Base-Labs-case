import io
import zipfile
from pathlib import Path

import duckdb
import requests

BASE_DIR = Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
DB_PATH = BASE_DIR / "data" / "duckdb" / "bibitor.duckdb"

FILES = {
    "purchases": "https://www.pwc.com/content/dam/pwc/us/en/careers/documents/PurchasesFINAL12312016csv.zip",
    "beg_inventory": "https://www.pwc.com/content/dam/pwc/us/en/careers/documents/BegInvFINAL12312016csv.zip",
    "purchase_prices": "https://www.pwc.com/content/dam/pwc/us/en/careers/documents/2017PurchasePricesDecCSV.zip",
    "vendor_invoices": "https://www.pwc.com/content/dam/pwc/us/en/careers/documents/VendorInvoices12312016csv.zip",
    "end_inventory": "https://www.pwc.com/content/dam/pwc/us/en/careers/documents/EndInvFINAL12312016csv.zip",
    "sales": "https://www.pwc.com/content/dam/pwc/us/en/careers/documents/SalesFINAL12312016csv.zip",
}

EXPECTED_CSVS = {
    "PurchasesFINAL12312016.csv": "purchases",
    "BegInvFINAL12312016.csv": "beg_inventory",
    "2017PurchasePricesDec.csv": "purchase_prices",
    "VendorInvoices12312016.csv": "vendor_invoices",
    "EndInvFINAL12312016.csv": "end_inventory",
    "SalesFINAL12312016.csv": "sales",
}

def download_and_extract(name: str, url: str) -> None:
    RAW_DIR.mkdir(parents=True, exist_ok=True)
    print(f"Downloading {name}...")
    r = requests.get(url, timeout=120)
    r.raise_for_status()

    print(f"Extracting {name}...")
    with zipfile.ZipFile(io.BytesIO(r.content)) as z:
        z.extractall(RAW_DIR)

def main():
    # 1) Download + extract all data
    for name, url in FILES.items():
        download_and_extract(name, url)

    # 2) Create DB and load raw tables
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    for csv_name, table in EXPECTED_CSVS.items():
        path = RAW_DIR / csv_name
        if not path.exists():
            raise FileNotFoundError(f"Expected CSV not found: {path}. Check downloads in {RAW_DIR}")
        print(f"Loading {csv_name} -> raw.{table}")
        con.execute(f"DROP TABLE IF EXISTS raw.{table};")
        con.execute(f"""
            CREATE TABLE raw.{table} AS
            SELECT * FROM read_csv_auto('{path.as_posix()}', HEADER=TRUE);
        """)

    con.close()
    print(f"✅ Done! DuckDB created at: {DB_PATH}")

if __name__ == "__main__":
    main()
