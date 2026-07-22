-- =============================================================
-- sql/mart/create_mart_schema.sql
-- MarketLens — Phase 3
-- =============================================================
-- PURPOSE:
--   Creates the mart schema and two tables:
--     1. mart.stock_daily_mart   → one row per stock per day (with computed metrics)
--     2. mart.stock_monthly_mart → one row per stock per month (aggregated)
--
-- ALIGNED WITH: MarketLens Spring Boot API column naming convention
--
-- WHY A SEPARATE MART SCHEMA?
--   raw.*  = source of truth. Append-only. Never touch.
--   mart.* = business-ready. Re-computable any time from raw.
--
--   If a transformation has a bug → fix the script → truncate + reload mart.
--   The raw data is always safe. This is the data warehouse pattern.
--
-- HOW TO RUN:
--   Option A (pgAdmin): Query Tool → paste → Run ▶
--   Option B (psql):    psql -U alahadu -d marketlens -f sql/mart/create_mart_schema.sql
-- =============================================================


-- ── 1. Create the mart schema ─────────────────────────────────

CREATE SCHEMA IF NOT EXISTS mart;


-- ── 2. stock_daily_mart ───────────────────────────────────────
-- One row per stock per trading day.
-- Contains all raw OHLCV columns PLUS computed metrics:
--   daily_return_pct  → how much % the stock moved that day
--   ma_7d             → 7-day moving average of close_price
--   ma_30d            → 30-day moving average of close_price
--   volatility_7d     → 7-day rolling standard deviation (risk metric)

CREATE TABLE IF NOT EXISTS mart.stock_daily_mart (

    id                  BIGSERIAL       PRIMARY KEY,

    -- Core fields (copied from raw for convenience — dashboard doesn't need to JOIN)
    trade_date          DATE            NOT NULL,
    symbol              VARCHAR(20)     NOT NULL,
    company_name        VARCHAR(150)    NOT NULL,

    open_price          NUMERIC(15, 4)  NOT NULL,
    high_price          NUMERIC(15, 4)  NOT NULL,
    low_price           NUMERIC(15, 4)  NOT NULL,
    close_price         NUMERIC(15, 4)  NOT NULL,
    volume              BIGINT          NOT NULL,

    -- Computed metrics (the value-add of the transformation layer)
    daily_return_pct    NUMERIC(10, 4),     -- NULL for first row per stock (no prev day)
    ma_7d               NUMERIC(15, 4),     -- 7-day moving average
    ma_30d              NUMERIC(15, 4),     -- 30-day moving average
    volatility_7d       NUMERIC(15, 6),     -- 7-day rolling std deviation

    -- Pipeline metadata
    transformed_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (symbol, trade_date)
);

CREATE INDEX IF NOT EXISTS idx_mart_daily_symbol
    ON mart.stock_daily_mart (symbol);

CREATE INDEX IF NOT EXISTS idx_mart_daily_date
    ON mart.stock_daily_mart (trade_date);

CREATE INDEX IF NOT EXISTS idx_mart_daily_symbol_date
    ON mart.stock_daily_mart (symbol, trade_date);


-- ── 3. stock_monthly_mart ─────────────────────────────────────
-- One row per stock per calendar month.
-- Aggregated view used for the monthly trend chart in Power BI.

CREATE TABLE IF NOT EXISTS mart.stock_monthly_mart (

    id                  BIGSERIAL       PRIMARY KEY,

    month_start         DATE            NOT NULL,   -- first day of the month e.g. 2024-03-01
    symbol              VARCHAR(20)     NOT NULL,
    company_name        VARCHAR(150)    NOT NULL,

    -- Monthly aggregations
    avg_close_price     NUMERIC(15, 4)  NOT NULL,
    min_close_price     NUMERIC(15, 4)  NOT NULL,
    max_close_price     NUMERIC(15, 4)  NOT NULL,
    month_open_price    NUMERIC(15, 4)  NOT NULL,   -- close of first trading day of month
    month_close_price   NUMERIC(15, 4)  NOT NULL,   -- close of last trading day of month
    monthly_return_pct  NUMERIC(10, 4),             -- (month_close - month_open) / month_open * 100
    total_volume        BIGINT          NOT NULL,
    trading_days        INTEGER         NOT NULL,   -- how many trading days in this month

    transformed_at      TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP,

    UNIQUE (symbol, month_start)
);

CREATE INDEX IF NOT EXISTS idx_mart_monthly_symbol
    ON mart.stock_monthly_mart (symbol);

CREATE INDEX IF NOT EXISTS idx_mart_monthly_month
    ON mart.stock_monthly_mart (month_start);


-- ── 4. Verify after running ───────────────────────────────────
-- Run this to confirm both tables were created:
--
-- SELECT table_schema, table_name
-- FROM information_schema.tables
-- WHERE table_schema = 'mart'
-- ORDER BY table_name;
--
-- Expected:
--   mart | stock_daily_mart
--   mart | stock_monthly_mart
