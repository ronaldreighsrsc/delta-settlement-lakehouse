# 🧠 MEMORY.md — Persistent Repository Memory for delta-settlement-lakehouse

## 👤 1. Candidate & Project Context
- **Candidate:** Ronald Solares Chuquera, Industrial Civil Engineer (Top 6%, Universidad de Tarapacá).
- **Target Role:** Remote International Data Engineer (Fintech, B2B Payments, Modern Data Stack).
- **Aspirational Goal:** Digital nomad lifestyle traveling the world with high-yield remote engineering positions.
- **Language Standard:** 100% International Technical English across code, docstrings, tests, and documentation.

## 📍 2. Current Project State
- **Phase:** Spec 001 + Spec 002 Fully Implemented & Verified (`26/26` pytest tests passing 100%).
- **GitHub Repository:** `https://github.com/ronaldreighsrsc/delta-settlement-lakehouse`
- **Core Architecture:**
  1. **Bronze Layer (`bronze_raw_events`):** Append/date-replace raw JSON stream ingestion partitioned by `ingestion_date` via Delta Lake.
  2. **Silver Conformance & DLQ (`silver_settlements`, `silver_quarantine_dlq`):** Data Contract Gate validating ISO 4217 currencies, SWIFT BICs, and non-positive amounts with automated quarantine routing.
  3. **Gold Layer (`dbt-duckdb` + OLAP):** Modular SQL staging (`stg_settlements`, `stg_quarantine_dlq`) and Kimball marts (`dim_corridors`, `fct_daily_settlements`, `fct_counterparty_exposure`) backed by YAML schema tests & singular invariants.
  4. **Dagster Orchestration:** Native Windows/Linux Software-Defined Assets (`bronze_raw_events -> silver_medallion_gate -> @dbt_assets`) with runtime metadata and in-process `dbtRunner` event streaming.
  5. **Interactive Typer CLI:** Commands `orchestrate`, `run-pipeline`, `inspect-dlq`, `reconcile`, and `audit-table` with Rich styling.

## ⚖️ 3. Key Decisions & Rationale
- **Delta Lake over Spark Cluster:** Using `deltalake` (Rust delta-rs engine) provides full ACID, partition pruning, and time travel without needing a 1 GB JVM runtime.
- **Dagster + `dagster-dbt` over Airflow:** Airflow lacks native Windows support (POSIX `fcntl`/`pwd`); Dagster compiles natively on Windows/Linux and integrates directly with dbt manifests.
- **DeltaAnalyticsBridge (`dbt_bridge.py`):** Hydrates DuckDB `raw_lakehouse.*` views directly from committed `_delta_log` snapshots via PyArrow (handling 0-row Silver batches safely without external DuckDB extension downloads).
- **Deterministic Chaos Generation:** Allows repeatable automated tests and predictable demonstration runs for recruiters.

## ⚠️ 4. Lessons Learned & Guardrails
- **Post-Mortem Guardrail (PyArrow Timestamps):** Always specify `pa.timestamp("us", tz="UTC")` explicitly to prevent timezone conversion discrepancies across Windows/Linux runtimes.
- **Precision Floating Point:** Financial amounts in domain models use `Decimal` for business invariants, casting to `float64` only at the physical storage boundary for Parquet vectorization.
- **DuckDB Windows Handles & Speed:** Explicitly release `dbt-duckdb` singleton `_ENV` handles after `dbtRunner.invoke()` and pin `<1.11` dbt wheels to avoid heavy experimental-parser source builds.
