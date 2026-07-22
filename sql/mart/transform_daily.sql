-- =============================================================
-- sql/mart/transform_daily.sql
-- MarketLens — Phase 3
-- =============================================================
-- PURPOSE:
--   Reads from raw.stock_prices_raw
--   Computes metrics using SQL window functions
--   Writes results into mart.stock_daily_mart
--
-- WINDOW FUNCTIONS — the most important SQL concept in data engineering:
--
--   A window function computes a value ACROSS related rows WITHOUT
--   collapsing them (unlike GROUP BY which reduces rows).
--
--   Syntax:
--     FUNCTION() OVER (
--         PARTITION BY symbol      ← reset the window for each stock
--         ORDER BY trade_date      ← within the window, sort by date
--         ROWS BETWEEN N PRECEDING
--              AND CURRENT ROW     ← how many rows to include
--     )
--
--   Functions used here:
--     LAG()        → look at the PREVIOUS row's value
--     AVG()        → rolling average over a window of rows
--     STDDEV_POP() → rolling standard deviation over a window
-- =============================================================


-- ── STEP 1: Clear existing mart data (full refresh) ───────────
-- We empty the mart table before reloading.
-- Safe because the mart is DERIVED from raw — always recomputable.
-- This pattern is called "full refresh."

TRUNCATE TABLE mart.stock_daily_mart RESTART IDENTITY;


-- ── STEP 2: Insert transformed data ──────────────────────────
-- One INSERT statement does all the heavy lifting.

INSERT INTO mart.stock_daily_mart (
    trade_date,
    symbol,
    company_name,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,
    daily_return_pct,
    ma_7d,
    ma_30d,
    volatility_7d,
    transformed_at
)
SELECT
    trade_date,
    symbol,
    company_name,
    open_price,
    high_price,
    low_price,
    close_price,
    volume,

    -- ── METRIC 1: Daily return % ──────────────────────────────
    -- Formula: (today_close - yesterday_close) / yesterday_close * 100
    --
    -- LAG(close_price, 1) OVER (PARTITION BY symbol ORDER BY trade_date)
    --   → gives you the close_price from the row immediately before
    --     this one, within the same stock (PARTITION BY symbol),
    --     sorted by date (ORDER BY trade_date).
    --
    -- The very first row per stock has no previous row → NULL.
    -- That is correct — you can't compute a return with no history.

    ROUND(
        (close_price - LAG(close_price, 1) OVER (
            PARTITION BY symbol ORDER BY trade_date
        ))
        / LAG(close_price, 1) OVER (
            PARTITION BY symbol ORDER BY trade_date
        ) * 100
    , 4)                                                AS daily_return_pct,

    -- ── METRIC 2: 7-day moving average ───────────────────────
    -- Average close_price across the last 7 trading days.
    -- "ROWS BETWEEN 6 PRECEDING AND CURRENT ROW" = current row + 6 before it.
    --
    -- Why moving averages matter:
    --   Raw prices are noisy — they jump up and down daily.
    --   MA7 smooths short-term noise → shows the recent trend.
    --   MA30 smooths more → shows the long-term trend.
    --
    --   When MA7 crosses above MA30 → bullish signal (price trending up).
    --   When MA7 crosses below MA30 → bearish signal (price trending down).

    ROUND(
        AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )
    , 4)                                                AS ma_7d,

    -- ── METRIC 3: 30-day moving average ──────────────────────

    ROUND(
        AVG(close_price) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
        )
    , 4)                                                AS ma_30d,

    -- ── METRIC 4: 7-day rolling volatility ───────────────────
    -- Standard deviation of close_price over last 7 days.
    --
    -- What it means:
    --   High std dev → price swings a lot → risky stock
    --   Low std dev  → price is stable   → safer stock
    --
    -- STDDEV_POP = population std dev (uses all rows in window).
    -- STDDEV_SAMP = sample std dev (divides by n-1 instead of n).
    -- We use POP here because we have the complete window, not a sample.

    ROUND(
        STDDEV_POP(close_price) OVER (
            PARTITION BY symbol
            ORDER BY trade_date
            ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
        )
    , 6)                                                AS volatility_7d,

    CURRENT_TIMESTAMP                                   AS transformed_at

FROM raw.stock_prices_raw
ORDER BY symbol, trade_date;
