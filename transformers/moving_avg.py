"""
transformers/moving_avg.py
---------------------------
Phase 3 — MarketLens
"""

import os
import psycopg2
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger

load_dotenv()


def get_connection():
    logger.info("🔌  Connecting to PostgreSQL...")
    conn = psycopg2.connect(
        host     = os.getenv("DB_HOST",     "localhost"),
        port     = os.getenv("DB_PORT",     "5432"),
        dbname   = os.getenv("DB_NAME",     "marketlens"),
        user     = os.getenv("DB_USER",     "postgres"),
        password = os.getenv("DB_PASSWORD", "")
    )
    conn.autocommit = False
    logger.success("✅  Connected")
    return conn


def setup_mart_schema(conn):
    logger.info("🏗️   Setting up mart schema...")
    sql_path = os.path.join(os.path.dirname(__file__), "..", "sql", "mart", "create_mart_schema.sql")
    with open(sql_path, "r") as f:
        ddl = f.read()
    cur = conn.cursor()
    cur.execute(ddl)
    conn.commit()
    cur.close()
    logger.success("✅  Mart schema and tables ready")


def transform_daily(conn):
    """
    Fills mart.stock_daily_mart with computed metrics.
    Runs SQL directly in Python — avoids semicolon-splitting issues.

    Metrics computed per row:
      daily_return_pct → (today_close - yesterday_close) / yesterday_close * 100
      ma_7d            → average close over last 7 trading days
      ma_30d           → average close over last 30 trading days
      volatility_7d    → std dev of close over last 7 trading days
    """
    logger.info("Transforming → mart.stock_daily_mart ...")

    cur = conn.cursor()

    # Step 1: clear existing data (full refresh pattern)
    cur.execute("TRUNCATE TABLE mart.stock_daily_mart RESTART IDENTITY")
    logger.info(" Cleared daily mart")

    # Step 2: insert with window functions
    # All SQL runs as ONE statement — no semicolon splitting needed
    cur.execute("""
        INSERT INTO mart.stock_daily_mart (
            trade_date, symbol, company_name,
            open_price, high_price, low_price, close_price, volume,
            daily_return_pct, ma_7d, ma_30d, volatility_7d,
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

            -- Daily return %
            -- LAG() looks at the previous row's close_price for same stock
            ROUND(
                (close_price - LAG(close_price, 1) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ))
                / LAG(close_price, 1) OVER (
                    PARTITION BY symbol ORDER BY trade_date
                ) * 100
            , 4)  AS daily_return_pct,

            -- 7-day moving average
            -- Average of today + 6 previous trading days
            ROUND(
                AVG(close_price) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                )
            , 4)  AS ma_7d,

            -- 30-day moving average
            ROUND(
                AVG(close_price) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 29 PRECEDING AND CURRENT ROW
                )
            , 4)  AS ma_30d,

            -- 7-day rolling volatility (std deviation = measure of risk)
            ROUND(
                STDDEV_POP(close_price) OVER (
                    PARTITION BY symbol
                    ORDER BY trade_date
                    ROWS BETWEEN 6 PRECEDING AND CURRENT ROW
                )
            , 6)  AS volatility_7d,

            CURRENT_TIMESTAMP

        FROM raw.stock_prices_raw
        ORDER BY symbol, trade_date
    """)

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM mart.stock_daily_mart")
    count = cur.fetchone()[0]
    cur.close()

    logger.success(f"mart.stock_daily_mart loaded: {count:,} rows")
    return count


def transform_monthly(conn):
    """
    Fills mart.stock_monthly_mart — one row per stock per month.
    Reads from mart.stock_daily_mart (already clean and validated).
    """
    logger.info("Transforming → mart.stock_monthly_mart ...")

    cur = conn.cursor()

    # Clear
    cur.execute("TRUNCATE TABLE mart.stock_monthly_mart RESTART IDENTITY")
    logger.info("Cleared monthly mart")

    # Insert monthly aggregations using a CTE (WITH clause)
    # CTE = Common Table Expression — a named subquery
    # Makes complex SQL readable by breaking it into steps
    cur.execute("""
        INSERT INTO mart.stock_monthly_mart (
            month_start, symbol, company_name,
            avg_close_price, min_close_price, max_close_price,
            month_open_price, month_close_price, monthly_return_pct,
            total_volume, trading_days, transformed_at
        )
        WITH monthly_base AS (
            -- Step 1: tag each row with its month's first and last close
            SELECT
                DATE_TRUNC('month', trade_date)::DATE   AS month_start,
                symbol,
                company_name,
                trade_date,
                close_price,
                volume,

                -- FIRST_VALUE: close price on first trading day of the month
                FIRST_VALUE(close_price) OVER (
                    PARTITION BY symbol, DATE_TRUNC('month', trade_date)
                    ORDER BY trade_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS first_close,

                -- LAST_VALUE: close price on last trading day of the month
                LAST_VALUE(close_price) OVER (
                    PARTITION BY symbol, DATE_TRUNC('month', trade_date)
                    ORDER BY trade_date
                    ROWS BETWEEN UNBOUNDED PRECEDING AND UNBOUNDED FOLLOWING
                ) AS last_close

            FROM mart.stock_daily_mart
        )
        -- Step 2: group by month and aggregate
        SELECT
            month_start,
            symbol,
            company_name,
            ROUND(AVG(close_price), 4)              AS avg_close_price,
            ROUND(MIN(close_price), 4)              AS min_close_price,
            ROUND(MAX(close_price), 4)              AS max_close_price,
            ROUND(MAX(first_close), 4)              AS month_open_price,
            ROUND(MAX(last_close),  4)              AS month_close_price,
            ROUND(
                (MAX(last_close) - MAX(first_close))
                / MAX(first_close) * 100
            , 4)                                    AS monthly_return_pct,
            SUM(volume)                             AS total_volume,
            COUNT(*)                                AS trading_days,
            CURRENT_TIMESTAMP                       AS transformed_at
        FROM monthly_base
        GROUP BY month_start, symbol, company_name
        ORDER BY symbol, month_start
    """)

    conn.commit()

    cur.execute("SELECT COUNT(*) FROM mart.stock_monthly_mart")
    count = cur.fetchone()[0]
    cur.close()

    logger.success(f"mart.stock_monthly_mart loaded: {count:,} rows")
    return count


def run_quality_checks(conn):
    logger.info("")
    logger.info("Running quality checks...")

    cur = conn.cursor()
    passed = 0
    failed = 0

    # Check 1: row count matches raw
    cur.execute("SELECT COUNT(*) FROM raw.stock_prices_raw")
    raw_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM mart.stock_daily_mart")
    mart_count = cur.fetchone()[0]
    if raw_count == mart_count:
        logger.success(f"Row count matches raw: {mart_count:,}"); passed += 1
    else:
        logger.warning(f"Mismatch: raw={raw_count} mart={mart_count}"); failed += 1

    # Check 2: no null trade_dates
    cur.execute("SELECT COUNT(*) FROM mart.stock_daily_mart WHERE trade_date IS NULL")
    n = cur.fetchone()[0]
    if n == 0:
        logger.success("No null trade_dates"); passed += 1
    else:
        logger.warning(f"   {n} null trade_dates"); failed += 1

    # Check 3: no null close prices
    cur.execute("SELECT COUNT(*) FROM mart.stock_daily_mart WHERE close_price IS NULL")
    n = cur.fetchone()[0]
    if n == 0:
        logger.success("No null close prices"); passed += 1
    else:
        logger.warning(f" {n} null close prices"); failed += 1

    # Check 4: NULL daily_return only for first row per stock
    cur.execute("SELECT COUNT(DISTINCT symbol) FROM raw.stock_prices_raw")
    stock_count = cur.fetchone()[0]
    cur.execute("SELECT COUNT(*) FROM mart.stock_daily_mart WHERE daily_return_pct IS NULL")
    n = cur.fetchone()[0]
    if n == stock_count:
        logger.success(f"NULL returns = {n} (correct — one per stock's first row)"); passed += 1
    else:
        logger.warning(f" NULL returns = {n} (expected {stock_count})"); failed += 1

    # Check 5: monthly coverage
    cur.execute("SELECT COUNT(DISTINCT month_start) FROM mart.stock_monthly_mart")
    months = cur.fetchone()[0]
    logger.success(f"Monthly mart covers {months} distinct months"); passed += 1

    # Check 6: no duplicates
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT symbol, trade_date, COUNT(*) AS cnt
            FROM mart.stock_daily_mart
            GROUP BY symbol, trade_date HAVING COUNT(*) > 1
        ) d
    """)
    dupes = cur.fetchone()[0]
    if dupes == 0:
        logger.success(" No duplicates in daily mart"); passed += 1
    else:
        logger.warning(f" {dupes} duplicate (symbol, date) pairs!"); failed += 1

    cur.close()
    logger.info(f"\n   Result: {passed} passed  |  {failed} failed")
    return failed == 0


def print_preview(conn):
    cur = conn.cursor()

    logger.info("")
    logger.info("=" * 70)
    logger.info("📊  PREVIEW — mart.stock_daily_mart  (latest 5 rows, AAPL)")
    logger.info("=" * 70)
    cur.execute("""
        SELECT trade_date, symbol, close_price,
               daily_return_pct, ma_7d, ma_30d, volatility_7d
        FROM mart.stock_daily_mart
        WHERE symbol = 'AAPL'
        ORDER BY trade_date DESC LIMIT 5
    """)
    logger.info(f"  {'date':<12} {'close':>9} {'return%':>10} {'MA7':>10} {'MA30':>10} {'vol7d':>12}")
    logger.info(f"  {'-'*65}")
    for r in cur.fetchall():
        logger.info(
            f"  {str(r[0]):<12} {float(r[2]):>9.2f} "
            f"{str(r[3]):>10} {str(r[4]):>10} {str(r[5]):>10} {str(r[6]):>12}"
        )

    logger.info("")
    logger.info("=" * 70)
    logger.info("📊  PREVIEW — mart.stock_monthly_mart  (last 3 months, all stocks)")
    logger.info("=" * 70)
    cur.execute("""
        SELECT month_start, symbol, avg_close_price,
               month_open_price, month_close_price,
               monthly_return_pct, trading_days
        FROM mart.stock_monthly_mart
        ORDER BY month_start DESC, symbol LIMIT 12
    """)
    logger.info(f"  {'month':<10} {'symbol':<15} {'avg':>9} {'open':>9} {'close':>9} {'ret%':>9} {'days':>5}")
    logger.info(f"  {'-'*68}")
    for r in cur.fetchall():
        logger.info(
            f"  {str(r[0])[:7]:<10} {r[1]:<15} {float(r[2]):>9.2f} "
            f"{float(r[3]):>9.2f} {float(r[4]):>9.2f} {str(r[5]):>9} {r[6]:>5}"
        )
    cur.close()


def main():
    logger.info("=" * 70)
    logger.info("🚀  MarketLens — Transformer  |  Phase 3")
    logger.info(f"📅  Run date : {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 70)

    conn = get_connection()
    setup_mart_schema(conn)
    logger.info("")

    daily_rows   = transform_daily(conn)
    monthly_rows = transform_monthly(conn)
    all_passed   = run_quality_checks(conn)
    print_preview(conn)

    logger.info("")
    logger.info("─" * 70)
    logger.info("TRANSFORMATION COMPLETE")
    logger.info(f"   mart.stock_daily_mart   : {daily_rows:,} rows")
    logger.info(f"   mart.stock_monthly_mart : {monthly_rows:,} rows")
    logger.info(f"   Quality checks          : {'All passed' if all_passed else 'Some failed'}")
    logger.info("─" * 70)
    logger.info("─" * 70)

    conn.close()


if __name__ == "__main__":
    main()
