# Annie’s Magic Numbers – Inventory Analysis (Base Labs Case)

This project ingests raw CSV files for a liquor distributor, transforms the data using SQL, and generates analytical reports to understand profits and margins by product and brand.

## Tech Stack
- Python (orchestration)
- DuckDB (analytical database)
- SQL (transformations)
- CSV outputs for reporting

## Project Structure

1. Download the CSV files from PwC:
   https://www.pwc.com/us/en/careers/university-relations/data-and-analytics-case-studies-files.html

2. Place the CSV files in:
   data/manual/

3. Run:
   ```bash
   python src/run_sql_pipeline.py
