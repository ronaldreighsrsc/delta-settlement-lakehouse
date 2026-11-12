# ⚡ Delta Settlement Lakehouse
> **Production-grade modern fintech Lakehouse engine for cross-border B2B payment settlements, built with Delta Lake, DuckDB, PyArrow, and Kimball dimensional modeling.**

![Python 3.12+](https://img.shields.io/badge/Python-3.12+-blue.svg?style=flat-square)
![Delta Lake](https://img.shields.io/badge/Storage-Delta%20Lake%20(delta--rs)-00ADD8.svg?style=flat-square)
![AWS S3](https://img.shields.io/badge/Cloud%20Lake-AWS%20S3%20%7C%20Moto%20%240-FF9900.svg?style=flat-square)
![Terraform](https://img.shields.io/badge/IaC-Terraform%20HCL-7B42BC.svg?style=flat-square)
![Compute Engine](https://img.shields.io/badge/Compute-DuckDB%20%2B%20PyArrow-FFF000.svg?style=flat-square)
![dbt](https://img.shields.io/badge/Transform-dbt--duckdb-FF694B.svg?style=flat-square)
![Dagster](https://img.shields.io/badge/Orchestrator-Dagster%20SDA-4F43DD.svg?style=flat-square)
![Airflow](https://img.shields.io/badge/Orchestrator-Airflow%202.10%20Docker-017CEE.svg?style=flat-square)
![Architecture](https://img.shields.io/badge/Pattern-Medallion%20(Bronze%20%E2%86%92%20Silver%20%E2%86%92%20Gold)-brightgreen.svg?style=flat-square)
![Data Governance](https://img.shields.io/badge/Governance-Data%20Contracts%20%2B%20DLQ-red.svg?style=flat-square)
![Tests](https://img.shields.io/badge/Tests-44%2F44%20Passing%20(100%25)-success.svg?style=flat-square)
![Zero JVM](https://img.shields.io/badge/Runtime-Zero--JVM%20Native%20Python-purple.svg?style=flat-square)

---

## 🎯 1. Executive Summary & Business Impact

In global cross-border B2B clearing and multi-currency commerce, financial platforms must process millions of fragmented payment events daily across disparate SWIFT and ACH networks. Traditional relational databases hit write-lock bottlenecks during peak batch reconciliations, while vanilla data lakes suffer from silent data corruption, lack of ACID guarantees, and inability to perform historical regulatory audits.

**Delta Settlement Lakehouse** resolves these challenges by combining the **ACID transactional guarantees of Delta Lake** with the **sub-second vectorized OLAP performance of DuckDB**:
* 🛡️ **Defensive Data Contract Gates:** Incoming raw streams are validated in-flight against ISO 4217 currency standards, SWIFT BIC formats, and decimal precision checks. Corrupted or malformed events are automatically diverted to a **Silver Quarantine Dead Letter Queue (DLQ)** without halting the pipeline.
* ⏱️ **Forensic Time Travel Auditability:** Full historical ledger snapshots via Delta Lake transaction logs (`_delta_log`), enabling instant `versionAsOf` queries to audit accounting state before and after late-arriving adjustments.
* 🚀 **Zero-JVM High-Performance Compute:** Runs entirely in native Python via Rust-backed `delta-rs` and vectorized C++ `duckdb`, processing large batches in under 3 seconds with zero cluster overhead.
* 📊 **Kimball Star Schema & Modular dbt SQL Marts:** Dimensional analytics (`dim_corridors`, `fct_daily_settlements`, `fct_counterparty_exposure`) built cleanly with `dbt-duckdb`, declarative YAML schema tests, and singular financial reconciliation assertions.
* 🔗 **Dual Orchestration (Dagster SDA + Containerized Apache Airflow):** Shared pure task primitives (`orchestration/tasks.py`) power both native Dagster `@asset`/`@dbt_assets` lineage and a containerized **Apache Airflow 2.10 (`LocalExecutor` + PostgreSQL)** TaskFlow DAG (`@dag` / `@task.external_python`).
* ☁️ **Terraform IaC & Dual `s3://` / Local Storage:** Declarative HCL (`infra/terraform/`) provisions 4 isolated Medallion S3 buckets (`bronze`, `silver`, `dlq`, `gold`), PublicAccessBlock, versioning, lifecycle retention, scoped IAM roles/policies, and toggleable AWS Budgets cost guardrails — runnable at **\$0 cost** against local Docker Moto (`s3-mock`) / LocalStack or promoted seamlessly to an **AWS Free Plan** account.

---

## 🏗️ 2. System Architecture (Medallion Pattern + Terraform S3 + dbt + Dagster & Airflow)

The system adheres strictly to Clean Architecture, decoupling domain entities, dual local/`s3://` storage abstractions, data quality gates, declarative SQL marts, Terraform cloud IaC, and multi-orchestrator DAG execution:

```mermaid
flowchart TD
    subgraph CloudIaC ["☁️ Infrastructure as Code (Terraform HCL — infra/terraform/)"]
        Terraform["hashicorp/terraform 1.9\n(local-docker | localstack | AWS Free Plan)"]
        S3Infra["4 Medallion S3 Buckets (bronze, silver, dlq, gold)\nPublicAccessBlock + Versioning + Lifecycle + IAM + Budget Guard"]
        Terraform --> S3Infra
    end

    subgraph Ingestion ["📡 Source Feeds & Chaos Generator"]
        RawEvents["Raw Payment Stream\n(SWIFT MT103 / ISO 20022 JSON)"]
        Chaos["Chaos Injector\n(Late Data, Malformed BICs, Bad Currencies)"]
    end

    subgraph Orchestration ["🧭 Dual DAG Orchestration (Shared tasks.py)"]
        SharedTasks["Framework-Agnostic Tasks\norchestration/tasks.py"]
        Dagster["Dagster Native Assets\n(@asset / @multi_asset / @dbt_assets)"]
        Airflow["Containerized Apache Airflow 2.10\n(Airflow LocalExecutor + cloud profile s3://)"]
    end

    subgraph StorageAbstraction ["💾 Dual Storage Engine (DeltaLakeStorage)"]
        Storage["delta-rs Backend\nLocal Disk OR s3://<zone-bucket>/table"]
    end

    subgraph Bronze ["🥉 Bronze Layer (Raw Ingestion)"]
        BronzeDelta[("Delta Table: bronze_raw_events\nPartitioned by ingestion_date\nImmutable Append / Date Replace")]
    end

    subgraph Governance ["🛡️ Data Quality & Contract Gates"]
        ContractGate{"Data Contract Validator\n(ISO 4217, BIC, Amount > 0)"}
        DLQ["Quarantine Router\n(Rejection Tagging)"]
    end

    subgraph Silver ["🥈 Silver Layer (Governed & Conformed)"]
        SilverDelta[("Delta Table: silver_settlements\nPartitioned by currency + YYYY-MM\nACID Idempotent")]
        DLQDelta[("Delta Table: silver_quarantine_dlq\nIsolated Corrupted Records")]
    end

    subgraph dbtAnalytics ["🥇 Gold SQL Analytics Layer (dbt-duckdb + Gold S3)"]
        Bridge["DeltaAnalyticsBridge\n(_delta_log via S3/Local -> DuckDB raw_lakehouse)"]
        Staging["Staging Views\nstg_settlements | stg_quarantine_dlq"]
        Marts["Kimball Marts & Quality Gates\ndim_corridors | fct_daily_settlements | fct_counterparty_exposure"]
    end

    S3Infra -. "Provisions Zones" .-> Storage
    RawEvents --> Chaos --> SharedTasks
    Dagster --> SharedTasks
    Airflow --> SharedTasks
    SharedTasks --> Storage --> BronzeDelta --> ContractGate
    ContractGate -- "Corrupted / Schema Violation" --> DLQ --> DLQDelta
    ContractGate -- "Conformed & Validated" --> SilverDelta
    SilverDelta & DLQDelta --> Bridge --> Staging --> Marts
```

---

## 🚀 3. Core Architectural Highlights

### A. Defensive Data Contract Gates & Dead Letter Queue (DLQ)
A junior engineer writes naive transform scripts that crash on dirty data. A senior data engineer builds **fault-tolerant contract gates**:
* Every transaction must comply with ISO 4217 currency lists (`USD`, `EUR`, `GBP`, `CLP`, `BRL`, `JPY`, etc.).
* SWIFT BICs are validated via strict ISO 9362 regular expressions (`^[A-Z]{6}[A-Z0-9]{2}([A-Z0-9]{3})?$`).
* Non-positive, zero, or unparseable monetary amounts are intercepted immediately.
* Corrupted records are enriched with deterministic error taxonomy (`ERR_INVALID_CURRENCY`, `ERR_INVALID_SWIFT_BIC`, `ERR_MALFORMED_JSON`) and committed to `silver_quarantine_dlq` for non-repudiation and compliance investigation.

### B. Delta Lake ACID & Time Travel Engine
Delta Lake maintains a transactional JSON log in `_delta_log/`. The engine supports historical state inspection:
```bash
# Audit historical table state at version 0 vs latest version
py -m delta_settlement_lakehouse.cli audit-table --table silver_settlements --version 0
```
This capability allows financial controllers to prove what the exact clearing balance was on a historical date prior to late-arriving settlement entries.

### C. Zero-Copy Vectorized OLAP with DuckDB & Modular `dbt` Marts
Gold layer dimensional models are transformed declaratively in SQL (`dbt-duckdb`) directly over PyArrow-hydrated Delta snapshots (`DeltaAnalyticsBridge`), enforcing schema contracts (`not_null`, `unique`, `accepted_values`) and financial parity invariants (`assert_reconciled_silver_parity.sql`) on every build.

### D. Dual DAG Orchestration (Dagster SDA + Containerized Apache Airflow)
`delta_settlement_lakehouse/orchestration/tasks.py` isolates pure Medallion task callables so both orchestrators share a single tested implementation:
* **Dagster Native Assets:** Coordinates Bronze ingestion, Silver/DLQ contract validation, and downstream `@dbt_assets` natively on Windows/Linux.
* **Apache Airflow 2.10 (Docker Compose):** Runs `settlement_medallion_dag` with PostgreSQL `LocalExecutor` and `@task.external_python` isolating Lakehouse/`dbt-core` dependencies in `/opt/lakehouse_venv` (zero `protobuf` dependency conflicts with Airflow's control plane).

### E. Terraform Infrastructure as Code & Dual `s3://` / Local Storage
`infra/terraform/` declaratively provisions the Lakehouse's cloud perimeter with **zero code fork** between `$0` local Docker emulation (`Moto s3-mock` or `LocalStack`) and a real **AWS Free Plan** account:
* **4 Dedicated Medallion Buckets:** `bronze`, `silver`, `dlq`, and `gold`, each configured with `aws_s3_bucket_public_access_block` (all 4 public blocks enabled), `aws_s3_bucket_versioning`, and noncurrent/aborted-upload lifecycle rules.
* **Least-Privilege IAM & Cost Guardrail:** Scoped IAM runtime role/policy restricted strictly to the 4 Medallion bucket ARNs, plus a toggleable `aws_budgets_budget` (`$1.00/mo` with 80% email threshold when targeting live AWS).
* **Dual `DeltaLakeStorage` (`delta-rs`):** Seamlessly resolves `s3://` zone URIs via `LAKEHOUSE_S3_*_BUCKET` environment variables and `storage_options`, while defaulting to native filesystem directories on local runs.

---

## ⚡ 4. Quickstart Guide (60 Seconds)

### Prerequisites
* Python 3.12+
* Optional for cloud / Airflow containers: Docker Desktop (`docker compose`)

### 1. Clone & Install Dependencies
```bash
git clone https://github.com/ronaldreighsrsc/delta-settlement-lakehouse.git
cd delta-settlement-lakehouse
pip install -r requirements.txt
```

### 2. Run the Full Test Suite
```bash
py -m pytest tests -v
```
*(44 unit, integration, dbt SQL, S3 dual-storage, Terraform HCL contract, Dagster DAG, Airflow AST/Docker Compose, and CLI tests passing at 100%)*.

### 3. Run the End-to-End Orchestrated DAG (Dagster + dbt-duckdb)
```bash
py -m delta_settlement_lakehouse.cli orchestrate --count 100 --date 2026-10-08 --chaos --chaos-ratio 0.08
```
*(Optional visual Dagster UI lineage explorer: `dagster dev -m delta_settlement_lakehouse.orchestration.definitions`)*

### 4. Run Direct Medallion Pipeline with Chaos Injection
```bash
py -m delta_settlement_lakehouse.cli run-pipeline --count 500 --date 2026-10-08 --chaos --chaos-ratio 0.08
```

### 5. Inspect Quarantined Corrupted Events (DLQ)
```bash
py -m delta_settlement_lakehouse.cli inspect-dlq --limit 10
```

### 6. View Gold Daily Reconciliations
```bash
py -m delta_settlement_lakehouse.cli reconcile --date 2026-10-08
```

### 7. Perform a Time Travel Audit
```bash
py -m delta_settlement_lakehouse.cli audit-table --table silver_settlements
```

### 8. Run Containerized Apache Airflow (Local Disk Mode)
```bash
docker compose build
docker compose up -d postgres
docker compose run --rm airflow-init
docker compose run --rm --no-deps airflow-scheduler airflow dags test settlement_medallion_dag 2026-10-28
# Or launch the full UI & scheduler stack at http://localhost:8080 (admin / admin via .env.example):
docker compose up -d airflow-webserver airflow-scheduler
```

### 9. Provision S3 Infrastructure with Terraform & Run Airflow over `s3://`

#### Option A: Zero-Cost Local Docker S3 (`s3-mock` Moto + Terraform + Airflow)
```bash
docker compose --profile cloud build
docker compose --profile cloud up -d postgres s3-mock
docker compose run --rm airflow-init
# Applies 19 resources (4 Medallion buckets + PublicAccessBlock + Versioning + Lifecycle + IAM):
docker compose --profile cloud run --rm terraform-init
# Executes the Airflow DAG writing Delta Lake tables directly to s3:// buckets & hydrating DuckDB:
docker compose --profile cloud run --rm --no-deps airflow-cloud airflow dags test settlement_medallion_dag 2026-10-28
# Tear down containers cleanly (avoid `-v` if you want to keep the cached Terraform AWS provider volume):
docker compose --profile cloud down --remove-orphans
```

#### Option B: Promote Identical HCL to Real AWS Free Plan (`$1.00` Budget Guardrail)
```bash
cp infra/terraform/environments/aws-free-tier.tfvars.example infra/terraform/prod.tfvars
# Edit infra/terraform/prod.tfvars with your globally unique project_prefix & alert email, then:
cd infra/terraform
terraform init
terraform apply -var-file=prod.tfvars
# Destroy all provisioned buckets & policies when finished:
terraform destroy -var-file=prod.tfvars
```

---

## 📂 5. Repository Directory Layout

```
delta-settlement-lakehouse/
├── airflow/
│   └── dags/
│       └── settlement_medallion_dag.py # Airflow 2.10 TaskFlow DAG (@dag / @task.external_python + S3 env)
├── dbt_analytics/                # Modular dbt-duckdb SQL analytics project
│   ├── dbt_project.yml           # Materializations (staging views, marts tables)
│   ├── profiles.yml              # DuckDB connection isolated via LAKEHOUSE_DUCKDB_PATH
│   ├── models/
│   │   ├── staging/              # sources.yml, schema.yml, stg_settlements.sql, stg_quarantine_dlq.sql
│   │   └── marts/                # dim_corridors.sql, fct_daily_settlements.sql, fct_counterparty_exposure.sql
│   └── tests/                    # Singular SQL invariants (positive amounts & Silver parity)
├── delta_settlement_lakehouse/   # Clean Architecture production package
│   ├── domain/
│   │   ├── models.py             # Pure dataclasses (RawPaymentEvent, SettlementRecord, DLQ)
│   │   └── constants.py          # ISO 4217 currencies, SWIFT regex, error codes
│   ├── engines/
│   │   ├── chaos_generator.py    # Deterministic synthetic feed & chaos injector
│   │   ├── contract_validator.py # Pure data quality contract evaluation
│   │   ├── delta_storage.py      # Dual Local / S3 Delta Lake wrapper (zone URIs, time travel, delete_where)
│   │   ├── olap_engine.py        # DuckDB vectorized in-memory aggregation engine
│   │   └── dbt_bridge.py         # Delta (Local/S3) -> DuckDB Arrow hydration & programmatic dbtRunner bridge
│   ├── services/
│   │   ├── bronze_service.py     # Append-only raw feed ingestion into Bronze
│   │   ├── silver_service.py     # Cleansing, contract gates, and DLQ routing
│   │   ├── gold_service.py       # Kimball dimensional reconciliations & exposures
│   │   └── pipeline_runner.py    # Medallion pipeline runner
│   ├── orchestration/
│   │   ├── tasks.py              # Shared framework-agnostic Medallion tasks (Dagster + Airflow)
│   │   ├── assets.py             # Dagster Bronze, Silver/DLQ @multi_asset, and @dbt_assets
│   │   └── definitions.py        # Dagster Definitions & run_orchestrated_pipeline entrypoint
│   └── cli.py                    # Typer & Rich interactive command-line interface
├── infra/
│   └── terraform/                # Declarative AWS S3 Medallion IaC
│       ├── versions.tf           # Terraform >=1.6 & hashicorp/aws ~>5.0 constraint
│       ├── providers.tf          # Dynamic endpoint routing (local S3 mock vs AWS)
│       ├── variables.tf          # Zone set, retention days, endpoint & budget variables
│       ├── main.tf               # 4 S3 buckets, PublicAccessBlock, Versioning, Lifecycle, IAM & Budget
│       ├── outputs.tf            # Zone URIs, bucket ARNs, IAM role & policy ARNs
│       └── environments/         # local-docker.tfvars, localstack.tfvars, aws-free-tier.tfvars.example
├── docs/
│   └── constitution.md           # 6 Immutable engineering invariants
├── specs/
│   ├── 001-core-medallion-lakehouse/
│   ├── 002-dbt-analytics-and-dag-orchestration/
│   ├── 003-airflow-container-dag/
│   └── 004-terraform-s3-lakehouse/
├── tests/                        # 100% automated Pytest test suite (44 tests)
├── Dockerfile.airflow            # Custom Airflow 2.10.5 image with isolated /opt/lakehouse_venv
├── Dockerfile.s3-mock            # Lightweight Moto S3/IAM emulator container for $0 cloud tests
├── docker-compose.yml            # Postgres 16 + Airflow + opt-in `cloud` profile (s3-mock, terraform-init)
├── .env.example                  # Documented local container & S3 override template
├── requirements.txt              # Production dependencies
├── AGENTS.md                     # Agentic guidelines and engineering rules
└── MEMORY.md                     # Persistent repository state
```

---

## ⚖️ 6. Engineering Decisions & Trade-Offs

| Decision | Alternative Discarded | Technical Rationale |
| :--- | :--- | :--- |
| **Delta Lake via `deltalake` (Rust)** | Apache Spark standalone cluster | `delta-rs` offers identical Delta Lake ACID table format and native `s3://` support in pure Python without requiring a heavy JVM cluster. |
| **Dual `s3://` / Local `DeltaLakeStorage`** | S3-only or local-only scripts | Encapsulating `table_uris`, `zone_buckets`, and `storage_options` inside `delta_storage.py` runs identical domain/dbt logic natively in Pytest and over S3 in cloud/Docker runs. |
| **Terraform Multi-Env IaC + Moto `s3-mock`** | Manual AWS Console clicks or ~1.5 GB LocalStack | Reproducible HCL applies cleanly at `$0` via `Dockerfile.s3-mock` (`moto[s3]`) and persistent `terraform_plugins` caching, while keeping `aws-free-tier.tfvars.example` + `$1.00` budget alerts ready for real AWS. |
| **Dual Orchestration (Dagster + Airflow Docker)** | Single framework lock-in | Dagster powers native Windows/Linux SDAs while containerized Apache Airflow (`LocalExecutor` + PostgreSQL) delivers industry-standard scheduled workflows backed by one shared `tasks.py` module. |
| **Isolated `@task.external_python` Venv** | Monolithic pip install inside Airflow | Isolates `dbt-core` (`protobuf>=6`) from Airflow's runtime (`opentelemetry protobuf<5`) cleanly via `/opt/lakehouse_venv` with zero dependency conflicts. |
| **`dbt-duckdb` over Delta Bridge** | Monolithic Python SQL strings | Decouples Kimball SQL marts (`dim_*`, `fct_*`), declarative YAML schema checks, and singular reconciliation invariants into testable analytics-engineering artifacts. |
| **Dead Letter Queue (DLQ)** | Silent record dropping | In enterprise financial architecture, dropping corrupted records without audit logging is a major regulatory compliance violation. |
| **Fixed-Point Decimal Arithmetic** | IEEE 754 Floats | Floating-point rounding drift is impermissible in foreign exchange settlement and ledger reconciliations. |

---

## 👤 Author
**Ronald Solares Chuquera**  
*Industrial Civil Engineer — Universidad de Tarapacá (Arica, Chile)*  
*Academic Distinction: Graduated 5th out of 79 (Top 6%)*  
[LinkedIn](https://www.linkedin.com/in/ronald-solares-chuquera-69232a150/) | [GitHub](https://github.com/ronaldreighsrsc)
