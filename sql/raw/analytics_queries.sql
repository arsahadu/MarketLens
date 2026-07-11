-- =============================================================
-- sql/raw/analytics_queries.sql
-- MarketLens — Phase 2 — Learning Queries
-- =============================================================
-- Run these in pgAdmin Query Tool against the marketlens database.
-- These use the Spring Boot aligned column names:
--   symbol, trade_date, close_price, open_price etc.
-- =============================================================


-- ─── QUERY 1: How much data do we have? ──────────────────────
SELECT
    symbol,
    company_name,
    COUNT(*)                    AS total_days,
    MIN(trade_date)             AS earliest_date,
    MAX(trade_date)             AS latest_date,
    ROUND(AVG(close_price), 2)  AS avg_close,
    ROUND(MIN(close_price), 2)  AS min_close,
    ROUND(MAX(close_price), 2)  AS max_close
FROM raw.stock_prices_raw
GROUP BY symbol, company_name
ORDER BY symbol;


-- ─── QUERY 2: Latest price per stock (ROW_NUMBER window function) ─
-- ROW_NUMBER() assigns rank 1 to the most recent row per stock.
-- PARTITION BY symbol → ranking resets for each stock.

SELECT symbol, company_name, trade_date, close_price, volume
FROM (
    SELECT *,
        ROW_NUMBER() OVER (PARTITION BY symbol ORDER BY trade_date DESC) AS rn
    FROM raw.stock_prices_raw
) ranked
WHERE rn = 1
ORDER BY symbol;


-- ─── QUERY 3: Daily return % using LAG() ─────────────────────
-- LAG(close_price, 1) → close price from the previous trading day.
-- Formula: ((today - yesterday) / yesterday) * 100

SELECT
    trade_date,
    symbol,
    close_price,
    LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY trade_date) AS prev_close,
    ROUND(
        (close_price - LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY trade_date))
        / LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY trade_date) * 100
    , 4) AS daily_return_pct
FROM raw.stock_prices_raw
ORDER BY symbol, trade_date DESC
LIMIT 20;


-- ─── QUERY 4: 7-day and 30-day moving averages ───────────────
-- AVG() OVER (ROWS BETWEEN 6 PRECEDING AND CURRENT ROW)
-- = average of current row + 6 rows before = 7-row sliding window.

SELECT
    trade_date,
    symbol,
    close_price,
    ROUND(
        AVG(close_price) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )
    , 2) AS ma_7d,
    ROUND(
        AVG(close_price) OVER (
            PARTITION BY symbol ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )
    , 2) AS ma_30d
FROM raw.stock_prices_raw
ORDER BY symbol, trade_date DESC
LIMIT 30;


-- ─── QUERY 5: Monthly average close ──────────────────────────
-- DATE_TRUNC('month', trade_date) → truncates to first of month.
-- e.g. '2024-03-15' → '2024-03-01'

SELECT
    symbol,
    DATE_TRUNC('month', trade_date) AS month,
    ROUND(AVG(close_price), 2)      AS avg_close,
    ROUND(MIN(close_price), 2)      AS month_low,
    ROUND(MAX(close_price), 2)      AS month_high,
    SUM(volume)                     AS total_volume
FROM raw.stock_prices_raw
GROUP BY symbol, DATE_TRUNC('month', trade_date)
ORDER BY symbol, month DESC
LIMIT 24;


-- ─── QUERY 6: Best trading days (biggest single-day gains) ───
SELECT
    symbol,
    trade_date,
    close_price,
    ROUND(
        (close_price - LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date))
        / LAG(close_price) OVER (PARTITION BY symbol ORDER BY trade_date) * 100
    , 2) AS daily_return_pct
FROM raw.stock_prices_raw
ORDER BY daily_return_pct DESC NULLS LAST
LIMIT 10;
