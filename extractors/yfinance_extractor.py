"""
extractors/yfinance_extractor.py
---------------------------------
Phase 1 — MarketLens

This script is your FIRST data engineering component.

What it does:
  1. Defines a list of stock tickers to track
  2. Pulls historical OHLCV data using the yfinance library
  3. Cleans and labels the data
  4. Saves each stock as a CSV file in the data/ folder

OHLCV stands for:
  O — Open   (price when the market opened that day)
  H — High   (highest price that day)
  L — Low    (lowest price that day)
  C — Close  (price when the market closed)
  V — Volume (how many shares were traded)

Run this from the project root:
  python extractors/yfinance_extractor.py
"""

import os
import time
import yfinance as yf
import pandas as pd
from datetime import datetime
from loguru import logger

# ─── CONFIG ────────────────────────────────────────────────────────────────────
# These are the stocks we want to track.
# ".NS" suffix = National Stock Exchange (India).  No suffix = US market (NASDAQ/NYSE).

STOCKS = {
    "AAPL":          "Apple Inc.",
    "TCS.NS":        "Tata Consultancy Services",
    "INFY.NS":       "Infosys",
    "RELIANCE.NS":   "Reliance Industries",
}

# How far back do we want data?  "5y" = 5 years of daily price history.
PERIOD = "5y"

# Where do we save the CSV files?
# os.path.dirname(__file__)  → folder where this script lives  → extractors/
# ".."                       → go one level up                 → marketlens/
# "data"                     → into the data folder            → marketlens/data/
DATA_DIR = os.path.join(os.path.dirname(__file__), "..", "data")


# ─── HELPER: DOWNLOAD ONE STOCK ────────────────────────────────────────────────

def download_stock(ticker: str, company_name: str) -> pd.DataFrame | None:
    """
    Downloads OHLCV data for one ticker using yfinance.

    Args:
        ticker       : e.g. "TCS.NS"
        company_name : e.g. "Tata Consultancy Services"

    Returns:
        A pandas DataFrame with columns:
            date, ticker, company, open, high, low, close, volume
        Or None if the download failed.
    """
    logger.info(f"⬇️  Downloading: {company_name} ({ticker})")

    try:
        # Use download() instead of Ticker().history() — more stable, less rate limiting
        df = yf.download(ticker, period=PERIOD, auto_adjust=True, progress=False)

        # If Yahoo Finance returned an empty table, something is wrong with the ticker.
        if df.empty:
            logger.warning(f"⚠️  No data returned for {ticker} — check the ticker symbol.")
            return None

        # ── CLEAN UP THE DATAFRAME ────────────────────────────────────────────
        # yfinance returns the Date as the INDEX, not a column.
        # reset_index() moves it into a proper column.
        df = df.reset_index()

        # Rename columns to lowercase — consistent naming is a DE best practice.
# yf.download returns MultiIndex columns — flatten them first
        if isinstance(df.columns, pd.MultiIndex):
            df.columns = [col[0].lower() for col in df.columns]
        else:
            df.columns = df.columns.str.lower()

        df["date"] = pd.to_datetime(df["date"]).dt.date

        # Add metadata columns so we know which stock each row belongs to.
        # Without these, a CSV with just open/high/low/close is useless.
        df["ticker"]  = ticker
        df["company"] = company_name

        # Keep only the columns we care about — in a clean order.
        df = df[["date", "ticker", "company", "open", "high", "low", "close", "volume"]]

        # Round price columns to 2 decimal places (money = 2 decimals).
        for col in ["open", "high", "low", "close"]:
            df[col] = df[col].round(2)

        logger.success(f"✅  {ticker}: {len(df)} rows fetched  |  {df['date'].min()} → {df['date'].max()}")
        return df

    except Exception as e:
        logger.error(f"❌  Failed to download {ticker}: {e}")
        return None


# ─── HELPER: SAVE TO CSV ───────────────────────────────────────────────────────

def save_to_csv(df: pd.DataFrame, ticker: str) -> str:
    """
    Saves a DataFrame as a CSV in the data/ folder.

    Filename pattern:  data/AAPL_raw_2024-01-05.csv
    The date in the filename = today's run date.
    This way, if you run the extractor multiple times, you get separate files
    and never overwrite history.  This is called 'partitioning by run date'.

    Args:
        df     : The cleaned DataFrame
        ticker : e.g. "TCS.NS"

    Returns:
        The file path where the CSV was saved.
    """
    # Sanitize ticker for filename: "TCS.NS" → "TCS_NS"
    safe_ticker = ticker.replace(".", "_")

    # Today's date as a string, e.g. "2024-06-27"
    run_date = datetime.today().strftime("%Y-%m-%d")

    filename = f"{safe_ticker}_raw_{run_date}.csv"
    filepath = os.path.join(DATA_DIR, filename)

    # index=False → don't write the row numbers (0, 1, 2...) as a column
    df.to_csv(filepath, index=False)

    logger.success(f"💾  Saved → {filepath}  ({len(df)} rows)")
    return filepath


# ─── MAIN ──────────────────────────────────────────────────────────────────────

def main():
    """
    Runs the full extraction for all stocks in STOCKS dict.
    This is the function that gets called when you run the script.
    """
    logger.info("=" * 60)
    logger.info("🚀  MarketLens — Stock Data Extractor  |  Phase 1")
    logger.info(f"📅  Run date  : {datetime.today().strftime('%Y-%m-%d %H:%M:%S')}")
    logger.info(f"📦  Stocks   : {', '.join(STOCKS.keys())}")
    logger.info(f"📂  Output   : {os.path.abspath(DATA_DIR)}")
    logger.info("=" * 60)

    # Make sure the data/ folder exists before we try to save into it.
    os.makedirs(DATA_DIR, exist_ok=True)

    saved_files  = []   # Track which files were successfully saved
    failed_tickers = [] # Track which tickers had errors

    for ticker, company_name in STOCKS.items():
        df = download_stock(ticker, company_name)

        if df is not None:
            filepath = save_to_csv(df, ticker)
            saved_files.append(filepath)
        else:
            failed_tickers.append(ticker)
        time.sleep(3)

    # ── SUMMARY REPORT ──────────────────────────────────────────────────────
    logger.info("")
    logger.info("─" * 60)
    logger.info("📊  EXTRACTION COMPLETE — Summary")
    logger.info("─" * 60)
    logger.info(f"✅  Successful : {len(saved_files)}/{len(STOCKS)} stocks")

    if saved_files:
        logger.info("📁  Files saved:")
        for f in saved_files:
            logger.info(f"     {f}")

    if failed_tickers:
        logger.warning(f"❌  Failed     : {', '.join(failed_tickers)}")

    logger.info("─" * 60)
    logger.info("💡  Next step: open the CSVs in data/ and inspect the columns.")
    logger.info("─" * 60)


# ─── ENTRY POINT ───────────────────────────────────────────────────────────────
# This is a Python convention.
# "__name__ == '__main__'" is True ONLY when you run this file directly.
# It is False when another file imports this module.
# This way, download_stock() and save_to_csv() can be imported and reused
# later by Airflow without accidentally running main() on import.

if __name__ == "__main__":
    main()
