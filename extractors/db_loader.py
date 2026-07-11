"""
extractors/db_loader.py
------------------------
Phase 2 — MarketLens

What this script does:
  1. Reads .env for PostgreSQL credentials
  2. Connects to PostgreSQL
  3. Creates raw schema + table (if not already there)
  4. Reads all CSVs from data/
  5. Loads them into raw.stock_prices_raw
  6. Skips duplicates (symbol + trade_date already in DB)
  7. Prints a data quality report

Column mapping (CSV → PostgreSQL):
  date    → trade_date
  ticker  → symbol
  company → company_name
  open    → open_price
  high    → high_price
  low     → low_price
  close   → close_price
  volume  → volume

Run from project root:
  python extractors/db_loader.py
"""

import os
import glob
import psycopg2
import pandas as pd
from datetime import datetime
from dotenv import load_dotenv
from loguru import logger

# Load .env file — reads DB_HOST, DB_PORT, DB_NAME, DB_USER, DB_PASSWORD
load_dotenv()

# ─── CONFIG ────────────────────────────────────────────────────────────────────

DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


# ─── STEP 1: CONNECT TO POSTGRESQL ─────────────────────────────────────────────

def get_connection():
    """
    Connects to PostgreSQL using credentials from .env file.

    os.getenv("DB_HOST") reads the value from your .env file.
    Never hardcode passwords in code — always use environment variables.
    """
    logger.info("🔌  Connecting to PostgreSQL...")

    conn = psycopg2.connect(
        host     = os.getenv("DB_HOST",     "localhost"),
        port     = os.getenv("DB_PORT",     "5432"),
        dbname   = os.getenv("DB_NAME",     "marketlens"),
        user     = os.getenv("DB_USER",     "postgres"),
        password = os.getenv("DB_PASSWORD", "")
    )

    # autocommit=False means we control transactions manually.
    # We call conn.commit() after each insert to save it permanently.
    conn.autocommit = False

    logger.success("✅  Connected to PostgreSQL")
    return conn


# ─── STEP 2: SETUP SCHEMA + TABLE ──────────────────────────────────────────────

def setup_database(conn):
    """
    Creates raw schema and stock_prices_raw table if they don't exist.
    Reads from sql/raw/create_raw_schema.sql.

    Safe to run multiple times — IF NOT EXISTS means no crash on re-run.
    """
    logger.info("🏗️   Setting up raw schema and table...")

    sql_path = os.path.join(
        os.path.dirname(__file__), "..", "sql", "raw", "create_raw_schema.sql"
    )

    with open(sql_path, "r") as f:
        ddl = f.read()

    cur = conn.cursor()
    cur.execute(ddl)
    conn.commit()
    cur.close()

    logger.success("✅  Schema and table ready")


# ─── STEP 3: LOAD ONE CSV ──────────────────────────────────────────────────────

def load_csv(conn, filepath: str) -> dict:
    """
    Loads one CSV file into raw.stock_prices_raw.

    Key concept — ON CONFLICT DO NOTHING:
      If (symbol, trade_date) already exists in the table,
      PostgreSQL silently skips that row instead of crashing.
      This makes the loader IDEMPOTENT — safe to run multiple times.

    Returns dict with: inserted, skipped, ticker
    """
    filename = os.path.basename(filepath)
    logger.info(f"📂  Loading: {filename}")

    stats = {"inserted": 0, "skipped": 0, "ticker": "unknown"}

    try:
        df = pd.read_csv(filepath)

        # Validate required columns
        required = {"date", "ticker", "company", "open", "high", "low", "close", "volume"}
        if not required.issubset(df.columns):
            missing = required - set(df.columns)
            logger.error(f"❌  Missing columns: {missing}")
            return stats

        stats["ticker"] = df["ticker"].iloc[0]

        # Convert date strings to proper date objects
        df["date"] = pd.to_datetime(df["date"]).dt.date

        cur = conn.cursor()

        # Count rows before insert — to calculate how many were actually inserted
        cur.execute("SELECT COUNT(*) FROM raw.stock_prices_raw WHERE symbol = %s",
                    (stats["ticker"],))
        before = cur.fetchone()[0]

        # Bulk insert using executemany
        # ON CONFLICT (symbol, trade_date) DO NOTHING = skip duplicates silently
        insert_sql = """
            INSERT INTO raw.stock_prices_raw
                (trade_date, symbol, company_name,
                 open_price, high_price, low_price, close_price,
                 volume, ingested_at)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (symbol, trade_date) DO NOTHING
        """

        rows = [
            (
                str(row["date"]),
                row["ticker"],
                row["company"],
                float(row["open"]),
                float(row["high"]),
                float(row["low"]),
                float(row["close"]),
                int(row["volume"]),
                datetime.now()
            )
            for _, row in df.iterrows()
        ]

        cur.executemany(insert_sql, rows)
        conn.commit()

        # Count after to calculate inserted vs skipped
        cur.execute("SELECT COUNT(*) FROM raw.stock_prices_raw WHERE symbol = %s",
                    (stats["ticker"],))
        after = cur.fetchone()[0]

        stats["inserted"] = after - before
        stats["skipped"]  = len(df) - stats["inserted"]

        cur.close()

        logger.success(
            f"✅  {stats['ticker']:15s} | "
            f"inserted: {stats['inserted']:4d} | "
            f"skipped (dup): {stats['skipped']:4d}"
        )

    except Exception as e:
        conn.rollback()
        logger.error(f"❌  Failed loading {filename}: {e}")

    return stats


# ─── STEP 4: DATA QUALITY CHECKS ───────────────────────────────────────────────

def verify_load(conn):
    """
    Runs SQL checks to confirm data loaded correctly.
    A real data engineer ALWAYS verifies after loading.
    """
    logger.info("")
    logger.info("🔍  Running data quality checks...")

    cur = conn.cursor()

    # Check 1 — total rows
    cur.execute("SELECT COUNT(*) FROM raw.stock_prices_raw")
    total = cur.fetchone()[0]
    logger.info(f"   Total rows       : {total:,}")

    # Check 2 — rows per stock
    cur.execute("""
        SELECT symbol, company_name, COUNT(*) AS rows,
               MIN(trade_date) AS earliest,
               MAX(trade_date) AS latest
        FROM raw.stock_prices_raw
        GROUP BY symbol, company_name
        ORDER BY symbol
    """)
    logger.info("   Rows per stock   :")
    for row in cur.fetchall():
        logger.info(f"     {row[0]:15s} | {row[2]:4d} rows | {row[3]} → {row[4]}")

    # Check 3 — null check
    cur.execute("""
        SELECT COUNT(*) FROM raw.stock_prices_raw
        WHERE trade_date IS NULL OR symbol IS NULL OR close_price IS NULL
    """)
    nulls = cur.fetchone()[0]
    if nulls == 0:
        logger.success("   Null check       : ✅ No nulls")
    else:
        logger.warning(f"   Null check       : ⚠️  {nulls} rows with nulls!")

    # Check 4 — duplicate check
    cur.execute("""
        SELECT COUNT(*) FROM (
            SELECT symbol, trade_date, COUNT(*) AS cnt
            FROM raw.stock_prices_raw
            GROUP BY symbol, trade_date
            HAVING COUNT(*) > 1
        ) dupes
    """)
    dupes = cur.fetchone()[0]
    if dupes == 0:
        logger.success("   Duplicate check  : ✅ No duplicates")
    else:
        logger.warning(f"   Duplicate check  : ⚠️  {dupes} duplicate (symbol, date) pairs!")

    # Check 5 — sample rows
    cur.execute("""
        SELECT trade_date, symbol, open_price, high_price, low_price, close_price, volume
        FROM raw.stock_prices_raw
        WHERE symbol = 'AAPL'
        ORDER BY trade_date DESC
        LIMIT 3
    """)
    logger.info("   Sample (AAPL latest 3):")
    for row in cur.fetchall():
        logger.info(
            f"     {row[0]} | O:{row[2]} H:{row[3]} L:{row[4]} C:{row[5]} | vol:{row[6]:,}"
        )

    cur.close()


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    logger.info("=" * 62)
    logger.info("🚀  MarketLens — DB Loader  |  Phase 2")
    logger.info(f"📅  Run date : {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info("=" * 62)

    conn = get_connection()
    setup_database(conn)

    # Find all raw CSVs
    csv_files = sorted(glob.glob(os.path.join(DATA_DIR, "*_raw_*.csv")))

    if not csv_files:
        logger.warning("⚠️  No CSVs found in data/ — run yfinance_extractor.py first")
        conn.close()
        return

    logger.info(f"📁  Found {len(csv_files)} CSV files")
    logger.info("")

    total_inserted = 0
    total_skipped  = 0

    for filepath in csv_files:
        stats = load_csv(conn, filepath)
        total_inserted += stats["inserted"]
        total_skipped  += stats["skipped"]

    verify_load(conn)

    logger.info("")
    logger.info("─" * 62)
    logger.info("📊  LOAD COMPLETE")
    logger.info(f"   Rows inserted : {total_inserted:,}")
    logger.info(f"   Rows skipped  : {total_skipped:,}  (already existed)")
    logger.info("─" * 62)
    logger.info("💡  Next: run python transformers/moving_avg.py")
    logger.info("─" * 62)

    conn.close()


if __name__ == "__main__":
    main()
