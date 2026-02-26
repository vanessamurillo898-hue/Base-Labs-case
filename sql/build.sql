CREATE SCHEMA IF NOT EXISTS raw;
CREATE SCHEMA IF NOT EXISTS stg;
CREATE SCHEMA IF NOT EXISTS mart;

-- ===== Staging: sales =====
DROP TABLE IF EXISTS stg.sales;
CREATE TABLE stg.sales AS
SELECT
  CAST("SalesDate" AS DATE) AS sales_date,
  CAST("Store" AS INTEGER) AS store,
  TRIM(CAST("Brand" AS VARCHAR)) AS brand,
  TRIM(CAST("Description" AS VARCHAR)) AS description,
  CAST("SalesQuantity" AS DOUBLE) AS qty_sold,
  CAST("SalesDollars" AS DOUBLE) AS sales_amount
FROM raw.sales;

-- ===== Staging: purchase prices =====
DROP TABLE IF EXISTS stg.purchase_prices;
CREATE TABLE stg.purchase_prices AS
SELECT
  TRIM(CAST("Brand" AS VARCHAR)) AS brand,
  TRIM(CAST("Description" AS VARCHAR)) AS description,
  CAST("Price" AS DOUBLE) AS unit_cost
FROM raw.purchase_prices;

-- Dedup prices
DROP TABLE IF EXISTS stg.purchase_prices_dedup;
CREATE TABLE stg.purchase_prices_dedup AS
SELECT
  brand,
  description,
  median(unit_cost) AS unit_cost
FROM stg.purchase_prices
GROUP BY 1,2;

-- ===== Fact =====
DROP TABLE IF EXISTS mart.fact_sales;
CREATE TABLE mart.fact_sales AS
SELECT
  s.sales_date,
  s.store,
  s.brand,
  s.description,
  s.qty_sold,
  s.sales_amount,
  p.unit_cost,
  (p.unit_cost * s.qty_sold) AS cogs_amount,
  (s.sales_amount - (p.unit_cost * s.qty_sold)) AS profit_amount,
  (s.sales_amount - (p.unit_cost * s.qty_sold)) / NULLIF(s.sales_amount, 0) AS margin_pct
FROM stg.sales s
LEFT JOIN stg.purchase_prices_dedup p
  ON s.brand = p.brand AND s.description = p.description;

-- ===== Aggregations =====
DROP TABLE IF EXISTS mart.agg_product;
CREATE TABLE mart.agg_product AS
SELECT
  brand,
  description,
  SUM(sales_amount) AS revenue,
  SUM(cogs_amount) AS cogs,
  SUM(profit_amount) AS profit,
  SUM(profit_amount) / NULLIF(SUM(sales_amount), 0) AS weighted_margin
FROM mart.fact_sales
WHERE unit_cost IS NOT NULL
GROUP BY 1,2;

DROP TABLE IF EXISTS mart.agg_brand;
CREATE TABLE mart.agg_brand AS
SELECT
  brand,
  SUM(sales_amount) AS revenue,
  SUM(cogs_amount) AS cogs,
  SUM(profit_amount) AS profit,
  SUM(profit_amount) / NULLIF(SUM(sales_amount), 0) AS weighted_margin
FROM mart.fact_sales
WHERE unit_cost IS NOT NULL
GROUP BY 1;
