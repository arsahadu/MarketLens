-- =============================================================
-- sql/raw/create_raw_schema.sql
-- MarketLens — Phase 2
-- =============================================================
-- PURPOSE:
--   Creates the "raw" schema and stock_prices_raw table in PostgreSQL.
--   This is the LANDING ZONE — data from yfinance arrives here
--   untransformed, exactly as it came from the source.
--
-- ALIGNED WITH: MarketLens Spring Boot API (StockPrice entity)
--   The column names here match the API's database table so both
--   the pipeline and the API read from the same PostgreSQL instance.
--
-- HOW TO RUN:
--   Option A (pgAdmin): Open Query Tool → paste this → Run
--   Option B (psql):    psql -U postgres -d marketlens -f create_raw_schema.sql
--
-- DDL vs DML (key concept):
--   DDL = CREATE, ALTER, DROP   → defines structure
--   DML = INSERT, UPDATE, SELECT → manipulates data
--   This file is pure DDL.
-- =============================================================


-- ── 1. Create the raw schema ──────────────────────────────────
-- A schema = a folder/namespace inside the database.
-- "IF NOT EXISTS" = safe to run multiple times (idempotent).

CREATE SCHEMA IF NOT EXISTS raw;


-- ── 2. Create stock_prices_raw table ─────────────────────────
-- Columns are aligned with the Spring Boot StockPrice JPA entity:
--
--   id          → Long id (auto-generated)
--   trade_date  → LocalDate tradeDate        (named trade_date to avoid SQL keyword clash)
--   symbol      → String symbol              (ticker symbol e.g. AAPL, TCS.NS)
--   company_name→ String companyName
--   open_price  → BigDecimal openPrice
--   high_price  → BigDecimal highPrice
--   low_price   → BigDecimal lowPrice
--   close_price → BigDecimal closePrice
--   volume      → Long volume

--   ingested_at → LocalDateTime ingestedAt   (pipeline metadata — when WE loaded this row)
--
-- Why NUMERIC and not FLOAT for prices?
--   FLOAT has binary rounding errors. 0.1 + 0.2 = 0.30000000000000004.
--   NUMERIC(15,4) = exact decimal. Always use this for money.
--
-- Why BIGINT for volume?
--   RELIANCE trades 10M+ shares/day. INT max = 2.1B. BIGINT = 9.2 quadrillion. Safe.

CREATE TABLE IF NOT EXISTS raw.stock_prices_raw (

    id              BIGSERIAL       PRIMARY KEY,

    trade_date      DATE            NOT NULL,
    symbol          VARCHAR(20)     NOT NULL,
    company_name    VARCHAR(150)    NOT NULL,

    open_price      NUMERIC(15, 4)  NOT NULL,
    high_price      NUMERIC(15, 4)  NOT NULL,
    low_price       NUMERIC(15, 4)  NOT NULL,
    close_price     NUMERIC(15, 4)  NOT NULL,
    volume          BIGINT          NOT NULL,

    -- Pipeline metadata
    ingested_at     TIMESTAMP       NOT NULL DEFAULT CURRENT_TIMESTAMP

);


-- ── 3. Indexes ────────────────────────────────────────────────
-- An index = the index at the back of a textbook.
-- Without it, PostgreSQL scans EVERY row to find your data.
-- With it, it jumps directly. Critical for 5000+ row tables.
--
-- We index symbol + trade_date because 90% of queries look like:
--   WHERE symbol = 'AAPL' AND trade_date BETWEEN '2024-01-01' AND '2024-12-31'

CREATE INDEX IF NOT EXISTS idx_raw_symbol
    ON raw.stock_prices_raw (symbol);

CREATE INDEX IF NOT EXISTS idx_raw_trade_date
    ON raw.stock_prices_raw (trade_date);

CREATE INDEX IF NOT EXISTS idx_raw_symbol_date
    ON raw.stock_prices_raw (symbol, trade_date);


-- ── 4. Unique constraint ──────────────────────────────────────
-- Business rule: ONE row per stock per day. No duplicates ever.
-- If the pipeline runs twice, the second run gets rejected silently
-- (using ON CONFLICT DO NOTHING in the Python loader).
-- The DATABASE enforces this rule — not just the Python code.

ALTER TABLE raw.stock_prices_raw
    DROP CONSTRAINT IF EXISTS uq_raw_symbol_date;

ALTER TABLE raw.stock_prices_raw
    ADD CONSTRAINT uq_raw_symbol_date
    UNIQUE (symbol, trade_date);


-- ── 5. Verify after running ───────────────────────────────────
-- Run this query to confirm the table was created correctly:
--
-- SELECT column_name, data_type, is_nullable
-- FROM information_schema.columns
-- WHERE table_schema = 'raw' AND table_name = 'stock_prices_raw'
-- ORDER BY ordinal_position;
--
-- Expected: 10 rows (id, trade_date, symbol, company_name,
--           open_price, high_price, low_price, close_price,
--           volume, ingested_at)
