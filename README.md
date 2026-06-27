# MarketLens — Financial Market Data Platform

> A beginner-to-intermediate Data Engineering project built to learn real-world pipeline concepts.

---

## What is MarketLens?

Companies like Zerodha, Groww, and Yahoo Finance all have a **backend data platform** before they can show you charts.  
MarketLens is a simplified version of that backend — built entirely by you, from scratch.

**I'm NOT building:**
- A trading app
- A stock prediction model
- A broker platform

**Building:**
- A data pipeline that **ingests** stock data automatically
- A **PostgreSQL warehouse** that stores it cleanly
- **Airflow** to orchestrate and schedule everything
- A **Power BI dashboard** that serves the analytics

---

## 🏗️ Architecture

```
[Data Sources]        Yahoo Finance (via yfinance library)
      ↓
[Ingestion Layer]     Python extractors, scheduled by Apache Airflow
      ↓
[Raw Storage]         PostgreSQL — raw schema (untransformed, append-only)
      ↓
[Transformation]      pandas + SQL — moving averages, % change, sector rollups
      ↓
[Warehouse / Mart]    PostgreSQL — mart schema (clean, aggregated)
      ↓
[Dashboard]           Power BI — charts, KPIs, trends
```

---

## Project Structure

```
marketlens/
├── dags/                  # Airflow DAG definitions (pipeline schedules)
│   ├── ingest_stocks.py
│   └── transform_stocks.py
├── extractors/            # Python scripts that pull raw data
│   └── yfinance_extractor.py
├── transformers/          # Data transformation logic
│   └── moving_avg.py
├── sql/
│   ├── raw/               # DDL for raw schema tables
│   └── mart/              # DDL for mart schema + transform queries
├── data/                  # Local landing zone for raw CSVs
├── dashboards/            # Power BI .pbix files
├── docker-compose.yml     # Spins up Airflow + PostgreSQL together
├── requirements.txt       # Python dependencies
└── README.md
```

---

## Getting Started

### 1. Clone the repo
```bash
git clone <your-repo-url>
cd marketlens
```

### 2. Create a Python virtual environment
```bash
python3 -m venv venv
source venv/bin/activate        # Mac/Linux
venv\Scripts\activate           # Windows
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. Run the extractor (Phase 1)
```bash
python extractors/yfinance_extractor.py
```
This pulls OHLCV data for AAPL, TCS, INFY, and RELIANCE.NS and saves CSVs to `data/`.

---

## Stocks Tracked

| Ticker       | Company              | Market |
|--------------|----------------------|--------|
| `AAPL`       | Apple Inc.           | NASDAQ |
| `TCS.NS`     | Tata Consultancy     | NSE    |
| `INFY.NS`    | Infosys              | NSE    |
| `RELIANCE.NS`| Reliance Industries  | NSE    |

---

## 🛠️ Tech Stack

| Layer        | Tool           |
|--------------|----------------|
| Extraction   | Python, yfinance|
| Orchestration| Apache Airflow |
| Storage      | PostgreSQL     |
| Container    | Docker         |
| Dashboard    | Power BI       |

---

*Built by Ahad — B.E. CSE, Thiagarajar College of Engineering, Madurai*
