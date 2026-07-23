"""
dags/marketlens_pipeline.py
-----------------------------
Phase 4 — MarketLens

This is the Airflow DAG (Directed Acyclic Graph) that orchestrates
the entire MarketLens pipeline automatically.

What is a DAG?
  A DAG defines TASKS and the ORDER they run in.
  "Directed"  = tasks flow in one direction (A → B → C)
  "Acyclic"   = no loops (A can't depend on C if C depends on A)
  "Graph"     = a network of connected tasks

Our pipeline has 3 tasks:

  task_extract → task_load → task_transform
      ↓               ↓            ↓
  yfinance       db_loader    moving_avg
  _extractor.py  .py          .py

Each task runs only after the previous one SUCCEEDS.
If task_extract fails → task_load never runs → you get an alert.

Schedule: runs every weekday at 6:00 PM IST (12:30 UTC)
          (Indian markets close at 3:30 PM IST)
"""

import os
import sys
from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.python import PythonOperator

# ── Add project root to Python path ─────────────────────────────
# Inside the Docker container, files are at /opt/airflow/
# We add these paths so our extractors/transformers are importable.
sys.path.insert(0, "/opt/airflow")
sys.path.insert(0, "/opt/airflow/extractors")
sys.path.insert(0, "/opt/airflow/transformers")

# ── Default arguments ────────────────────────────────────────────
# These apply to ALL tasks in the DAG unless overridden.

default_args = {
    "owner": "ahad",               # who owns this pipeline

    # If a scheduled run is missed (e.g. laptop was off),
    # don't go back and run all the missed runs.
    "depends_on_past": False,

    # Email on failure (set up SMTP later for real alerts)
    "email_on_failure": False,
    "email_on_retry": False,

    # If a task fails, retry once after 5 minutes
    "retries": 1,
    "retry_delay": timedelta(minutes=5),
}

# ── DAG definition ───────────────────────────────────────────────

dag = DAG(
    # Unique ID — shows as the DAG name in Airflow UI
    dag_id="marketlens_pipeline",

    description="MarketLens: Extract → Load → Transform stock data daily",

    default_args=default_args,

    # When did this DAG start existing?
    start_date=datetime(2024, 1, 1),

    # Cron schedule: "30 12 * * 1-5"
    #   30   = minute 30
    #   12   = hour 12 (UTC) = 6:00 PM IST
    #   *    = every day of month
    #   *    = every month
    #   1-5  = Monday to Friday only
    #
    # Cron format: minute hour day month weekday
    # Useful tool: https://crontab.guru
    schedule_interval="30 12 * * 1-5",

    # Don't run all missed runs since start_date
    catchup=False,

    # Tags appear in the Airflow UI for filtering
    tags=["marketlens", "stocks", "data-engineering"],
)


# ── TASK 1: Extract ──────────────────────────────────────────────
# Calls the same download_stock() and save_to_csv() functions
# from yfinance_extractor.py — no code duplication.

def run_extraction(**context):
    """
    Airflow calls this function when task_extract runs.
    **context gives access to Airflow metadata like run date.

    We import and call the extractor's main() directly.
    Same code, now automated.
    """
    from loguru import logger

    logger.info("=" * 60)
    logger.info(f"🚀  TASK 1: Extract  |  {context['ds']}")
    logger.info("=" * 60)

    # Import the extractor module
    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "yfinance_extractor",
        "/opt/airflow/extractors/yfinance_extractor.py"
    )
    extractor = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(extractor)

    # Run it
    extractor.main()
    logger.info("✅  Extraction complete")


# ── TASK 2: Load ─────────────────────────────────────────────────
# Loads CSVs from data/ into raw.stock_prices_raw in PostgreSQL.

def run_load(**context):
    """
    Calls db_loader.main() to load CSVs into PostgreSQL.
    Uses the Docker environment variables for DB connection.
    """
    import os
    from loguru import logger

    logger.info("=" * 60)
    logger.info(f"🚀  TASK 2: Load  |  {context['ds']}")
    logger.info("=" * 60)

    # Override DB connection to use Docker service name
    # Inside Docker, services talk to each other by service name
    # "marketlens-db" — not "localhost"
    os.environ["DB_HOST"]     = os.getenv("MARKETLENS_DB_HOST", "marketlens-db")
    os.environ["DB_PORT"]     = os.getenv("MARKETLENS_DB_PORT", "5432")
    os.environ["DB_NAME"]     = os.getenv("MARKETLENS_DB_NAME", "marketlens")
    os.environ["DB_USER"]     = os.getenv("MARKETLENS_DB_USER", "airflow")
    os.environ["DB_PASSWORD"] = os.getenv("MARKETLENS_DB_PASSWORD", "airflow")

    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "db_loader",
        "/opt/airflow/extractors/db_loader.py"
    )
    loader = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(loader)

    loader.main()
    logger.info("✅  Load complete")


# ── TASK 3: Transform ────────────────────────────────────────────
# Runs moving_avg.py to fill mart schema with computed metrics.

def run_transform(**context):
    """
    Calls moving_avg.main() to compute and load the mart tables.
    """
    import os
    from loguru import logger

    logger.info("=" * 60)
    logger.info(f"🚀  TASK 3: Transform  |  {context['ds']}")
    logger.info("=" * 60)

    os.environ["DB_HOST"]     = os.getenv("MARKETLENS_DB_HOST", "marketlens-db")
    os.environ["DB_PORT"]     = os.getenv("MARKETLENS_DB_PORT", "5432")
    os.environ["DB_NAME"]     = os.getenv("MARKETLENS_DB_NAME", "marketlens")
    os.environ["DB_USER"]     = os.getenv("MARKETLENS_DB_USER", "airflow")
    os.environ["DB_PASSWORD"] = os.getenv("MARKETLENS_DB_PASSWORD", "airflow")

    import importlib.util
    spec = importlib.util.spec_from_file_location(
        "moving_avg",
        "/opt/airflow/transformers/moving_avg.py"
    )
    transformer = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(transformer)

    transformer.main()
    logger.info("✅  Transform complete")


# ── Wire up tasks into the DAG ───────────────────────────────────
# PythonOperator wraps a Python function as an Airflow task.

task_extract = PythonOperator(
    task_id="extract_stock_data",       # shows in Airflow UI
    python_callable=run_extraction,     # function to run
    dag=dag,
)

task_load = PythonOperator(
    task_id="load_to_postgres",
    python_callable=run_load,
    dag=dag,
)

task_transform = PythonOperator(
    task_id="transform_to_mart",
    python_callable=run_transform,
    dag=dag,
)

# ── Set task order ───────────────────────────────────────────────
# The >> operator means "then run"
# This is the core of the DAG — the dependency chain.
#
#   extract_stock_data >> load_to_postgres >> transform_to_mart
#
# Read as:
#   "Run extract, THEN load, THEN transform"
#   If extract fails → load never starts → transform never starts

task_extract >> task_load >> task_transform
