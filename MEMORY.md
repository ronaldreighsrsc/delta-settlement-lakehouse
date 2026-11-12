# 🧠 MEMORY.md — Persistent Repository Memory for delta-settlement-lakehouse

## 👤 1. Candidate & Project Context
- **Candidate:** Ronald Solares Chuquera, Industrial Civil Engineer (Top 6%, Universidad de Tarapacá).
- **Target Role:** Remote International Data Engineer (Fintech, B2B Payments, Modern Data Stack).
- **Language Standard:** 100% International Technical English across code, docstrings, tests, and documentation.

## 📍 2. Current Project State
- **Phase:** Spec 001–004 Implemented & Verified (`44/44` native pytest tests + live Docker cloud E2E: Terraform `19` resources on Moto `s3-mock` + `airflow-cloud` DAG `PASS=30`).
- **GitHub Repository:** `https://github.com/ronaldreighsrsc/delta-settlement-lakehouse`
- **Core Architecture:**
  1. **Bronze Layer (`bronze_raw_events`):** Date-scoped idempotent raw JSON ingestion partitioned by `ingestion_date` via Delta Lake with batch-date salted seed determinism.
  2. **Silver Conformance & DLQ (`silver_settlements`, `silver_quarantine_dlq`):** Data Contract Gate validating ISO 4217 currencies, SWIFT BICs, and positive amounts with quarantine routing.
  3. **Gold Layer (`dbt-duckdb` + OLAP):** Modular SQL staging views (`stg_settlements`, `stg_quarantine_dlq`) and Kimball marts (`dim_corridors`, `fct_daily_settlements`, `fct_counterparty_exposure`) backed by YAML schema & singular SQL parity tests.
  4. **Dual Orchestration (`orchestration/tasks.py`):** Shared pure task callables driving both **Dagster** native assets and **Apache Airflow 2.10 (`LocalExecutor` + PostgreSQL)** via TaskFlow `@dag` / `@task.external_python`.
  5. **Terraform IaC & Dual `s3://`/Local Storage (`infra/terraform/` + `DeltaLakeStorage`):** Declarative HCL provisioning 4 Medallion S3 buckets (`bronze`, `silver`, `dlq`, `gold`), PublicAccessBlock, versioning, lifecycle retention, least-privilege IAM, and toggleable `aws_budgets_budget` (`\$1.00`) targeting local Moto (`s3-mock:5000`), LocalStack, or AWS Free Plan via `.tfvars`.

## ⚖️ 3. Key Decisions & Rationale
- **Slim Moto S3 Emulator (`Dockerfile.s3-mock`):** Uses `moto[s3]>=5.0` atop the existing Airflow image layer for fast `$0` S3+IAM compatibility (`local-docker.tfvars`), avoiding heavy 1.5 GB LocalStack pulls while preserving `localstack.tfvars` and `aws-free-tier.tfvars.example`.
- **Terraform Provider Cache (`terraform_plugins` volume):** `terraform-init` uses offline `-plugin-dir=/root/.terraform.d/plugin-cache` whenever `registry.terraform.io` exists; avoid `docker compose down -v` so cached `hashicorp/aws v5.0.0` isn't wiped.
- **Airflow `@task.external_python` + `/opt/lakehouse_venv`:** Isolates `dbt-core` (`protobuf>=6`) and S3/`deltalake` runtime from Airflow's control plane (`opentelemetry protobuf<5`).
- **Lazy PEP 562 `orchestration/__init__.py` (`__getattr__`):** Prevents `/opt/lakehouse_venv` from importing `dagster` when loading shared `orchestration.tasks`.
- **Date-Salted Batch Seed (`_derive_batch_seed`):** Prevents duplicate `transaction_id`/`event_id` collisions across multi-day scheduled Airflow runs.
- **DeltaAnalyticsBridge (`dbt_bridge.py`):** Hydrates DuckDB `raw_lakehouse.*` views via PyArrow snapshots from either local disk or `s3://` zone buckets with explicit handle cleanup.

## ⚠️ 4. Lessons Learned & Guardrails
- Always specify `pa.timestamp("us", tz="UTC")` explicitly across Windows/Linux PyArrow tables.
- Keep `Decimal` fixed-point math in domain invariants, casting to `float64` only at physical Parquet storage boundaries.
- Release `dbt-duckdb` singleton `_ENV` handles after `dbtRunner.invoke()` and pin `<1.11` dbt wheels to avoid Windows file locks and slow source builds.
