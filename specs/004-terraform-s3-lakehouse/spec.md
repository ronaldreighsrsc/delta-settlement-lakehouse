# Spec 004 — Terraform S3 Cloud Infrastructure & `s3://` Lakehouse Backend

**Status:** implemented  
**Author:** Ronald Solares  
**Date:** 2026-11-11  
**Project:** `delta-settlement-lakehouse` (`C:\Users\ronal\delta-settlement-lakehouse`)  

---

## 1. Context and Objective
Following Spec 003 (Containerized Apache Airflow + shared Medallion tasks), `delta-settlement-lakehouse` runs end-to-end orchestration locally over filesystem directories (`data/lakehouse`). To eliminate the "local-only pipeline" limitation and validate cloud Data Engineering & Infrastructure-as-Code (IaC) workflows at **\$0 cost**, **Spec 004** provisions Medallion AWS S3 buckets (`bronze`, `silver`, `dlq`, `gold`), security controls, lifecycle policies, least-privilege IAM, and optional AWS billing budget guardrails using **Terraform**, and enables `DeltaLakeStorage`, orchestration tasks, and Airflow to read/write ACID Delta tables directly over `s3://` URIs against both local Docker S3 emulation (**Moto `s3-mock`** / **LocalStack**) and real **AWS Free Plan** accounts.

---

## 2. Actors & Target Personas
* **Data / Platform Engineer:** Provisions reproducible S3 + IAM infrastructure using Terraform HCL, toggles between \$0 local Docker S3 testing (`s3-mock` Moto / `LocalStack`) and AWS Cloud using `.tfvars` variables only, and runs Airflow/CLI jobs over `s3://` Medallion zones.
* **CI / Windows Contributor:** Validates Delta storage routing, Terraform HCL contracts, and Docker Compose topology via native `py -m pytest` without requiring Docker or AWS cloud credentials during unit tests.

---

## 3. User Stories (Vertical Slices)
* **US-1 (Medallion S3 & Dual-Mode `DeltaLakeStorage`):** As a Data Engineer, I want `DeltaLakeStorage` and the shared Medallion pipeline tasks to operate seamlessly over `s3://` bucket URIs (or Medallion per-zone bucket overrides) via `delta-rs` S3 `storage_options`, while keeping local disk execution 100% intact when no S3 configuration is set.
* **US-2 (Terraform Medallion IaC Module with Security & Budget Guardrails):** As a Platform Engineer, I want clean Terraform configurations (`infra/terraform/`) provisioning 4 Medallion buckets (`bronze`, `silver`, `dlq`, `gold`), strict public access blocking, versioning, lifecycle retention rules, scoped IAM role/policy, and an AWS Budgets alert guard, parameterized for both local S3 emulation (`s3-mock` / `LocalStack`) and AWS Free Plan accounts.
* **US-3 (Live Containerized S3 + Terraform + Airflow Execution & `job_copilot` Alignment):** As a Candidate/Engineer, I want an opt-in Docker Compose workflow applying Terraform against containerized S3 and executing `settlement_medallion_dag` end-to-end onto live `s3://` Delta tables, followed by updating `job_copilot` with verified `Terraform (IaC)` and `AWS S3 (Data Lake)` skills.

---

## 4. Functional Requirements (EARS Notation)

### Cloud Storage Backend (`s3://` & Local Compatibility)
* **RF-1:** **WHEN** configured with an `s3://` URI prefix or per-zone Medallion bucket mapping (`bronze`, `silver`, `dlq`, `gold`), **THE SYSTEM** resolves `bronze_raw_events`, `silver_settlements`, `silver_quarantine_dlq`, and `gold_daily_reconciliation` to their target `s3://` URIs and forwards S3 `storage_options` across writes, reads, `is_deltatable`, `history()`, and partition-scoped `delete_where()` operations.
* **RF-2:** **WHILE** configured with a filesystem directory (default `data/lakehouse` or test temp path), **THE SYSTEM** preserves identical local filesystem Delta Lake behavior with zero AWS network dependencies or credentials required.
* **RF-3:** **WHEN** orchestration tasks (`execute_bronze_ingestion`, `execute_silver_contract_gate`), `DeltaAnalyticsBridge.hydrate_sources`, Dagster assets, or the Airflow TaskFlow DAG execute under S3 environment variables, **THE SYSTEM** commits Delta transaction logs and partitioned Parquet files to S3 and hydrates DuckDB `raw_lakehouse.*` tables from those S3 Delta snapshots cleanly before `dbt build`.

### Terraform Infrastructure as Code (`infra/terraform/`)
* **RF-4:** **THE SYSTEM** defines reproducible Terraform HCL provisioning four dedicated Medallion S3 buckets (`bronze`, `silver`, `dlq`, `gold`), each enforcing `aws_s3_bucket_public_access_block` (all four public blocks `true`), `aws_s3_bucket_versioning` (`Enabled`), noncurrent/temp prefix `aws_s3_bucket_lifecycle_configuration` rules, and a least-privilege IAM role + policy scoped exclusively to those four bucket ARNs.
* **RF-5:** **WHEN** configured for real AWS (`enable_budget_guard = true`), **THE SYSTEM** provisions an `aws_budgets_budget` monthly cost guardrail (`\$1.00 USD` ceiling with 80% email threshold), and **WHEN** configured for local \$0 Docker validation (`use_local_endpoint = true`, `enable_budget_guard = false`), **THE SYSTEM** applies the exact same S3/IAM HCL against a local S3 endpoint with zero cloud charges.

### Containerized Cloud Runtime & Portfolio Alignment
* **RF-6:** **WHEN** `docker compose --profile cloud` executes (`s3-mock` + `terraform-init` + `airflow-cloud`), **THE SYSTEM** initializes the four S3 buckets via Terraform, runs `airflow dags test settlement_medallion_dag`, commits real Bronze/Silver/DLQ Delta files into S3, and builds Gold DuckDB marts with zero test/SQL failures.

---

## 5. Non-Functional Requirements
* **RNF-1 (Zero-Regression Baseline):** All 32 pre-existing tests plus the 12 Spec 004 storage/Terraform/Compose contract tests (`44` total) pass 100% natively on Windows Python 3.12+ without Docker or AWS access.
* **RNF-2 (Security & Clean Repo Boundaries):** Zero real AWS credentials committed; `.terraform/`, `*.tfstate*`, secret `.tfvars`, and `.env` remain excluded in `.gitignore`. Both local S3 and AWS execution share the same Terraform code path (`environments/*.tfvars`).
* **RNF-3 (Truthful Portfolio Alignment):** `Terraform (IaC)` and `AWS S3 (Data Lake)` are added to `job_copilot` (`VERIFIED_SKILLS`, catalog bullets/tags, CV LaTeX rendering, LinkedIn post copy, and TDD tests) **only after** live Terraform + Airflow `s3://` execution completes cleanly.

---

## 6. Edge Cases
1. Empty Silver batch (`chaos_ratio=1.0`, 100% DLQ) running against `s3://` must still succeed in `silver_contract_gate` and `dbt_analytics_build`, writing `silver_quarantine_dlq` to the `dlq` bucket while leaving `silver_settlements` cleanly handled if absent.
2. Idempotent re-execution of the same `batch_date` against `s3://` (`delete_where` before `append`) must handle `is_deltatable` cleanly both on an empty newly-created S3 bucket and on subsequent partition overwrites without duplicate rows.
3. Terraform local provider configuration (`AWS_S3_ALLOW_UNSAFE_RENAME`, path-style addressing, custom endpoint) must remain isolated in configuration/env vars so switching to AWS production requires only changing endpoint/tfvars, never Python code edits.

---

## 7. Out of Scope
* Multi-node Spark/EMR clusters, Glue Data Catalog, Redshift, or paid Managed Workflows (MWAA).
* Provisioning remote Terraform state backend buckets (`backend "s3"`) interactively against paid AWS in this iteration (documented cleanly as commented remote-state pattern ready for AWS Free Plan).

---

## 8. Resolved Architecture Decisions (Implemented & Verified)
* **[RESOLVED - Slim Local S3 Runtime]:** Uses **`Dockerfile.s3-mock`** (`settlement-lakehouse-s3-mock:5.x` with `moto[s3]>=5.0` atop the cached Airflow base image) + **`hashicorp/terraform:1.9`** + persistent `terraform_plugins` cache volume inside `docker compose --profile cloud` (`environments/local-docker.tfvars`), replacing heavyweight ~1.5 GB LocalStack pulls while preserving `environments/localstack.tfvars` compatibility.
* **[RESOLVED - Multi-Bucket Zone Routing]:** Provisions **4 separate Medallion S3 buckets** (`bronze`, `silver`, `dlq`, `gold`) and routes each Delta table (`bronze_raw_events`, `silver_settlements`, `silver_quarantine_dlq`, `gold_daily_reconciliation`) cleanly by zone URI in `DeltaLakeStorage`, preserving single-path `base_path` compatibility.
* **[VERIFIED - Live `s3://` Pipeline Execution]:** Live containerized E2E verified idempotent Terraform apply (`19` resources), idempotent `airflow-cloud` DAG execution (`PASS=30` dbt tests), and `GoldService.reconcile_date` over `s3://` (`bronze=100`, `silver=88`, `dlq=12`, `gold=3`, `duckdb_tx=88`), unlocking `job_copilot` `Terraform (IaC)` and `AWS S3 (Data Lake)` alignment.

