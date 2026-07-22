-- =============================================================
-- sql/mart/transform_monthly.sql
-- MarketLens — Phase 3
-- =============================================================
-- PURPOSE:
--   Aggregates mart.stock_daily_mart → mart.stock_monthly_mart
--   One row per stock per calendar month.
--
-- Reads from mart.stock_daily_mart (not raw) because the daily
-- mart already has clean, validated data. No need to re-read raw.
--
-- New SQL concepts used:
--   DATE_TRUNC   → truncate a date to the start of the month
--   FIRST_VALUE  → get the first value within a window
--   LAST_VALUE   → get the last value within a window
-- =============================================================


-- ── STEP 1: Clear existing monthly mart ───────────────────────

TRUNCATE TABLE mart.stock_monthly_mart RESTART IDENTITY;


-- ── STEP 2: Insert monthly aggregations ──────────────────────

INSERT INTO mart.stock_monthly_mart (
    month_start,
    symbol,
    company_name,
    avg_close_price,
    min_close_price,
    max_close_price,
    month_open_price,
    month_close_price,
    monthly_return_pct,
    total_volume,
    trading_days,
    transformed_at
)
SELECT
    -- DATE_TRUNC('month', trade_date):
    --   '2024-03-15' → '2024-03-01'
    --   '2024-03-28' → '2024-03-01'
    --   All days in March collapse to the same month_start.
    --   This is how we GROUP BY calendar month.
    DATE_TRUNC('month', trade_date)::DATE           AS month_start,

    symbol,
    company_name,

    ROUND(AVG(close_price),  4)                     AS avg_close_price,
    ROUND(MIN(close_price),  4)                     AS min_close_price,
    ROUND(MAX(close_price),  4)                     AS max_close_price,

    -- month_open_price:
    --   The close_price on the FIRST trading day of the month.
    --   FIRST_VALUE() gets the first value in the window.
    --   We partition by symbol + month so the window resets
    --   for each stock for each month.
    ROUND(
        FIRST_VALUE(close_price) OVER (
            PARTITION BY symbol, DATE_TRUNC('month', trade_date)
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    , 4)                                            AS month_open_price,

    -- month_close_price:
    --   The close_price on the LAST trading day of the month.
    ROUND(
        LAST_VALUE(close_price) OVER (
            PARTITION BY symbol, DATE_TRUNC('month', trade_date)
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        )
    , 4)                                            AS month_close_price,

    -- Monthly return %:
    --   How much did the stock gain or lose this month?
    --   Formula: (last_day_close - first_day_close) / first_day_close * 100
    ROUND(
        (
            LAST_VALUE(close_price) OVER (
                PARTITION BY symbol, DATE_TRUNC('month', trade_date)
                ORDER BY trade_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
            -
            FIRST_VALUE(close_price) OVER (
                PARTITION BY symbol, DATE_TRUNC('month', trade_date)
                ORDER BY trade_date
                ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
            )
        )
        /
        FIRST_VALUE(close_price) OVER (
            PARTITION BY symbol, DATE_TRUNC('month', trade_date)
            ORDER BY trade_date
            ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
        ) * 100
    , 4)                                            AS monthly_return_pct,

    SUM(volume)                                     AS total_volume,
    COUNT(*)                                        AS trading_days,
    CURRENT_TIMESTAMP                               AS transformed_at

FROM mart.stock_daily_mart
GROUP BY
    symbol,
    company_name,
    DATE_TRUNC('month', trade_date),
    -- These must be in GROUP BY because they're window functions
    -- inside aggregation context in PostgreSQL
    FIRST_VALUE(close_price) OVER (
        PARTITION BY symbol, DATE_TRUNC('month', trade_date)
        ORDER BY trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    ),
    LAST_VALUE(close_price) OVER (
        PARTITION BY symbol, DATE_TRUNC('month', trade_date)
        ORDER BY trade_date
        ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
    )
ORDER BY symbol, month_start;
