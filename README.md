# Annie’s Magic Numbers — Inventory Analysis (SQL-first)

## Overview
This project ingests raw CSVs (Sales, Purchase Prices) into DuckDB and performs SQL-first transformations to compute:
- Profit ($) and margin (%) at product and brand level
- Top 10 products and brands by profit and margin
- Loss-making products and brands to consider dropping

Python is used only to orchestrate ingestion and execution. All business logic lives in SQL.

## How to run
1. Place CSVs in `data/manual/`:
   - SalesFINAL*.csv
   - *PurchasePrices*.csv
2. Run:
   ```bash
   python src/run_sql_pipeline.py
