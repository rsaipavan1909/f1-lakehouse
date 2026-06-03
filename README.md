# Formula 1 Analytics Lakehouse Platform

![CI](https://github.com/rsaipavan1909/f1-lakehouse/actions/workflows/ci.yml/badge.svg)

## Live Dashboard
🔗 [F1 Analytics Lakehouse Dashboard](https://public.tableau.com/app/profile/sai.pavan.rambhatla/viz/F1AnalyticsLakehouse/F1AnalyticsLakehouse20182024)

## Overview
An end-to-end Formula 1 analytics platform ingesting 70+ years of race data
using a modern data lakehouse architecture. Built to demonstrate production-grade
data engineering skills including pipeline orchestration, dimensional modeling,
data quality validation, and CI/CD.

## Tech Stack
| Layer | Technology |
|---|---|
| Ingestion | Python, FastF1 API |
| Storage | Delta Lake, DuckDB |
| Transformation | PySpark, dbt |
| Orchestration | Apache Airflow |
| Dashboard | Tableau Public |
| CI/CD | GitHub Actions |
| Version Control | Git, GitHub |

## Architecture
```
FastF1 API
    ↓
Bronze Layer (Delta Lake — raw partitioned JSON)
    ↓
Silver Layer (DuckDB — cleaned, typed, deduplicated)
    ↓
Gold Layer (dbt — dimensional model, KPI aggregations)
    ↓
Tableau Dashboard
```

## Medallion Architecture
- **Bronze** — Raw F1 data from FastF1, partitioned by year, with ingestion metadata
- **Silver** — Cleaned and typed tables with data quality flags and deduplication
- **Gold** — Star schema with fact and dimension tables powering the dashboard

## Key Features
- 70+ years of F1 race data (1950–2024)
- Medallion architecture: Bronze → Silver → Gold
- Automated pipeline with Apache Airflow DAGs
- SCD Type 2 driver dimension tracking team changes over seasons
- Data quality checks at every layer
- Incremental loading strategy
- CI/CD with GitHub Actions — dbt tests run on every push
- Live Tableau dashboard with driver and constructor KPIs

## Pipeline DAG
```
bronze_ingestion
    ↓              ↓
silver_laps    silver_results
    ↓              ↓
    data_quality_checks
           ↓
      load_to_duckdb
           ↓
     dbt_gold_models
           ↓
        dbt_tests
```

## Dashboard KPIs
- 2024 Driver Championship Standings
- Wins and Podiums by Driver
- Championship Battle by Round
- Constructor Points Share
- Driver Performance Heatmap (2018–2024)

## Project Structure
```
f1-lakehouse/
├── ingestion/              # Bronze layer — FastF1 ingestion scripts
├── transformations/        # Silver layer — DuckDB transformation scripts
├── dbt/f1_gold/
│   └── models/             # Gold layer — dbt dimensional models
├── airflow/
│   └── dags/               # Airflow pipeline DAGs
├── dashboards/
│   └── data/               # CSV exports for Tableau
├── tests/                  # Unit tests
└── .github/workflows/      # CI/CD GitHub Actions
```

## Setup Instructions
```bash
# Clone the repo
git clone https://github.com/rsaipavan1909/f1-lakehouse.git
cd f1-lakehouse

# Create virtual environment
python -m venv venv
source venv/bin/activate

# Install dependencies
pip install -r requirements.txt

# Run Bronze ingestion
python ingestion/bronze_ingestion.py

# Run Silver transformations
python transformations/silver_laps_duckdb.py
python transformations/silver_results_duckdb.py

# Load to DuckDB
python transformations/load_to_duckdb.py

# Run dbt Gold models
cd dbt/f1_gold
dbt run
dbt test

# Export for Tableau
python dashboards/export_for_powerbi.py
```

## Resume Highlights
- Built an end-to-end Formula 1 analytics platform ingesting 70+ years of race data
- Implemented medallion architecture using Bronze, Silver, and Gold layers
- Processed millions of lap-time and telemetry records using PySpark and DuckDB
- Orchestrated automated pipelines with Apache Airflow DAGs
- Designed dimensional models with SCD Type 2 and KPI aggregations using dbt
- Implemented data quality validation and incremental loading strategies
- Built CI/CD pipeline with GitHub Actions running automated dbt tests on every push
- Published interactive Tableau dashboard with driver and constructor performance KPIs
