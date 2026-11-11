# 🧠 MEMORY.md — Persistent Repository Memory for delta-settlement-lakehouse

## 👤 1. Candidate & Project Context
- **Candidate:** Ronald Solares Chuquera, Industrial Civil Engineer (Top 6%, Universidad de Tarapacá).
- **Target Role:** Remote International Data Engineer (Fintech, B2B Payments, Modern Data Stack).
- **Language Standard:** 100% International Technical English across code, docstrings, tests, and documentation.

## 📍 2. Current Project State
- **Phase:** Spec 001 + Spec 002 + Spec 003 Fully Implemented & Verified (`32/32` pytest tests passing 100% + live Docker Airflow DAG run verified).
- **GitHub Repository:** `https://github.com/ronaldreighsrsc/delta-settlement-lakehouse`
- **Core Architecture:**
  1. **Bronze Layer (`bronze_raw_events`):** Date-scoped idempotent raw JSON ingestion partitioned by `ingestion_date` via Delta Lake with batch-date salted seed determinism.
  2. **Silver Conformance & DLQ (`silver_settlements`, `silver_quarantine_dlq`):** Data Contract Gate validating ISO 4217 currencies, SWIFT BICs, and positive amounts with quarantine routing.
  3. **Gold Layer (`dbt-duckdb` + OLAP):** Modular SQL staging views (`stg_settlements`, `stg_quarantine_dlq`) and Kimball marts (`dim_corridors`, `fct_daily_settlements`, `fct_counterparty_exposure`) backed by YAML schema & singular SQL parity tests.
  4. **Dual Orchestration (`orchestration/tasks.py`):** Shared pure task callables driving both **Dagster** native assets and **Apache Airflow 2.10 (`LocalExecutor` + PostgreSQL)** via TaskFlow `@dag` / `@task.external_python`.

## ⚖️ 3. Key Decisions & Rationale
- **Airflow `@task.external_python` + `/opt/lakehouse_venv`:** Spike T0 proved `dbt-core>=1.9` requires `protobuf>=6` whereas Airflow 2.10 pins `opentelemetry-proto` (`protobuf<5`). Isolating Lakehouse/`dbt` dependencies in `/opt/lakehouse_venv` keeps `pip check` 100% clean in both environments.
- **Lazy PEP 562 `orchestration/__init__.py` (`__getattr__`):** Prevents `/opt/lakehouse_venv` from importing `dagster` when loading shared `orchestration.tasks`.
- **Date-Salted Batch Seed (`_derive_batch_seed`):** Prevents duplicate `transaction_id`/`event_id` collisions across multi-day scheduled Airflow runs using default `seed=42`.
- **DeltaAnalyticsBridge (`dbt_bridge.py`):** Hydrates DuckDB `raw_lakehouse.*` views via PyArrow snapshots with explicit handle cleanup on Windows.

## ⚠️ 4. Lessons Learned & Guardrails
- Always specify `pa.timestamp("us", tz="UTC")` explicitly across Windows/Linux PyArrow tables.
- Keep `Decimal` fixed-point math in domain invariants, casting to `float64` only at physical Parquet storage boundaries.
- Release `dbt-duckdb` singleton `_ENV` handles after `dbtRunner.invoke()` and pin `<1.11` dbt wheels to avoid Windows file locks and slow source builds.
