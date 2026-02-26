from pathlib import Path
import duckdb

BASE = Path(__file__).resolve().parents[1]
DATA_DIR = BASE / "data" / "manual"
DB_PATH = BASE / "data" / "duckdb" / "bibitor.duckdb"
SQL_PATH = BASE / "sql" / "build.sql"
OUT_DIR = BASE / "output"

def find_csv(keyword: str) -> Path:
    files = list(DATA_DIR.glob("*.csv"))
    for f in files:
        if keyword.lower() in f.name.lower():
            return f
    raise FileNotFoundError(f"No CSV in {DATA_DIR} matching {keyword}. Found: {[x.name for x in files]}")

def main():
    DB_PATH.parent.mkdir(parents=True, exist_ok=True)
    OUT_DIR.mkdir(parents=True, exist_ok=True)

    sales_csv = find_csv("sales")
    prices_csv = find_csv("purchase")

    con = duckdb.connect(str(DB_PATH))
    con.execute("CREATE SCHEMA IF NOT EXISTS raw;")

    con.execute("DROP TABLE IF EXISTS raw.sales;")
    con.execute(f"CREATE TABLE raw.sales AS SELECT * FROM read_csv_auto('{sales_csv.as_posix()}', HEADER=TRUE);")

    con.execute("DROP TABLE IF EXISTS raw.purchase_prices;")
    con.execute(f"CREATE TABLE raw.purchase_prices AS SELECT * FROM read_csv_auto('{prices_csv.as_posix()}', HEADER=TRUE);")

    con.execute(SQL_PATH.read_text(encoding="utf-8"))

    con.execute("COPY (SELECT * FROM mart.agg_product ORDER BY profit DESC LIMIT 10) TO 'output/top_products_profit.csv' (HEADER, DELIMITER ',');")
    con.execute("COPY (SELECT * FROM mart.agg_product ORDER BY weighted_margin DESC LIMIT 10) TO 'output/top_products_margin.csv' (HEADER, DELIMITER ',');")
    con.execute("COPY (SELECT * FROM mart.agg_brand ORDER BY profit DESC LIMIT 10) TO 'output/top_brands_profit.csv' (HEADER, DELIMITER ',');")
    con.execute("COPY (SELECT * FROM mart.agg_brand ORDER BY weighted_margin DESC LIMIT 10) TO 'output/top_brands_margin.csv' (HEADER, DELIMITER ',');")
    con.execute("COPY (SELECT * FROM mart.agg_product WHERE profit < 0 ORDER BY profit ASC) TO 'output/losing_products.csv' (HEADER, DELIMITER ',');")
    con.execute("COPY (SELECT * FROM mart.agg_brand WHERE profit < 0 ORDER BY profit ASC) TO 'output/losing_brands.csv' (HEADER, DELIMITER ',');")

    con.close()
    print("✅ SQL pipeline done. Check /output")

if __name__ == "__main__":
    main()
