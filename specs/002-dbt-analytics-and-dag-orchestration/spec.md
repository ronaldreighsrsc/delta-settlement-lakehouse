# Spec 002 — Modular SQL Analytics Layer & Asset DAG Orchestration

**Status:** approved  
**Author:** Ronald Solares  
**Date:** 2026-11-11  
**Project:** `delta-settlement-lakehouse` (`C:\Users\ronal\delta-settlement-lakehouse`)  

---

## 1. Context and Objective
Spec 001 delivered a deterministic Medallion Lakehouse (`Bronze -> Silver/DLQ -> Gold`) using embedded DuckDB queries inside Python services. While fast, enterprise Data Engineering and Analytics Engineering teams separate ingestion/contract enforcement from declarative SQL transformations, automated schema/business-invariant tests, and asset-level DAG orchestration.

**Spec 002** extends `delta-settlement-lakehouse` with a modular SQL transformation & governance layer plus an observable dependency DAG orchestrator linking raw ingestion, Delta Silver/DLQ processing, and dimensional Gold marts with automatic lineage tracking and failure isolation.

---

## 2. Actors & Target Personas
* **Analytics Engineer:** Maintains modular SQL staging and dimensional mart models (`dim_*`, `fct_*`) with declarative tests and documented column lineage.
* **Data Platform / MLOps Engineer:** Triggers, monitors, and retries orchestrated multi-step pipelines locally or in CI without fragile shell/cron scripts.

---

## 3. User Stories (Vertical Slices)
* **US-1 (Modular Staging & Gold SQL Marts):** As an Analytics Engineer, I want Silver settlement and DLQ datasets exposed to modular SQL staging views and dimensional marts (`dim_corridors`, `fct_daily_settlements`, `fct_counterparty_exposure`), so analytical transformations are versioned, readable, and decoupled from Python code.
* **US-2 (Declarative Data Quality Tests):** As a Data Governance Lead, I want automated tests validating uniqueness, non-null keys, accepted ISO 4217 currency/status domains, and financial reconciliation invariants on every build, so corrupted marts fail before publication.
* **US-3 (Asset DAG Orchestration & CLI Trigger):** As a Data Engineer, I want a declarative DAG orchestrator coordinating Bronze ingestion, Silver/DLQ contract validation, SQL marts, and automated data tests via CLI (`lakehouse orchestrate`) and visual DAG inspection, capturing execution counts and asset lineage.

---

## 4. Functional Requirements (EARS Notation)

### Modular SQL Analytics Layer
* **RF-1:** **WHEN** the analytics build executes over existing `silver_settlements` and `silver_quarantine_dlq` Delta datasets, **THE SYSTEM** materializes modular staging models (`stg_settlements`, `stg_quarantine_dlq`) and dimensional Gold marts (`dim_corridors`, `fct_daily_settlements`, `fct_counterparty_exposure`).
* **RF-2:** **THE SYSTEM** persists model documentation, dependencies, and lineage metadata cleanly after each build.

### Declarative Schema & Singular Business Tests
* **RF-3:** **WHEN** the analytics verification suite runs, **THE SYSTEM** executes declarative schema tests (`not_null`, `unique`, `accepted_values` for currencies and settlement statuses) and singular business assertions (no negative settled amounts, daily settled counts reconcile strictly with Silver totals).
* **RF-4:** **IF** any schema or invariant assertion fails, **THEN THE SYSTEM** terminates downstream step execution with a non-zero status and surfaced failure details.

### Asset DAG Orchestration
* **RF-5:** **WHEN** an operator invokes `lakehouse orchestrate` (or runs the orchestrator directly), **THE SYSTEM** materializes upstream Python/Delta assets (`bronze_raw_events`, `silver_settlements`, `silver_quarantine_dlq`) followed strictly by downstream SQL analytics and test assets in topological order.
* **RF-6:** **THE SYSTEM** attaches structured runtime metadata (record counts, DLQ rejections, settlement date, batch checksum/duration) to each materialized DAG step.

---

## 5. Non-Functional Requirements
* **RNF-1 (Cross-Platform Native Compatibility):** Automated testing and local pipeline execution must run natively on Windows Python 3.12+ and Linux CI runners without requiring Docker, WSL2, or remote cloud credentials.
* **RNF-2 (Deterministic & Fast Verification):** Full end-to-end orchestration tests must complete in under 15 seconds inside `py -m pytest`.
* **RNF-3 (Zero-Regression Baseline):** All 17 existing Spec 001 tests must remain 100% green.

---

## 6. Edge Cases
1. Empty Silver batch (e.g., 100% of ingested transactions rejected into DLQ) must handle empty SQL staging gracefully without crashing dimensional aggregation.
2. Re-running the orchestrated DAG for the same `batch_date` must remain idempotent without generating duplicate reconciliation rows.
3. Deliberate corruption injected into analytical output during test verification must cause automated data tests to fail deterministically.

---

## 7. Out of Scope
* Provisioning remote cloud resources (Terraform/AWS S3/Glue) — deferred to Spec 003.
* Hosting a 24/7 Kubernetes Airflow cluster.

---

## 8. Open Clarifications (Navigator Approval Required)
* **[NEEDS APPROVAL - Orchestrator]:** Select **Dagster (`dagster` + `dagster-dbt`)** as the orchestrator over Apache Airflow, because Airflow fails natively on Windows (requires POSIX `fcntl`/`pwd` via WSL2/Docker), whereas Dagster runs natively on Windows Python 3.12 & Linux CI, integrates natively with `dbt-duckdb` Software-Defined Assets, and offers visual lineage UI (`dagster dev`). *(Optional: confirm if you also want an Airflow DAG reference stub or strictly verified Dagster assets per YAGNI).*
* **[NEEDS APPROVAL - External Dependencies]:** Authorize adding `dbt-duckdb`, `dagster`, and `dagster-dbt` to `requirements.txt`.
* **[NEEDS APPROVAL - `job_copilot` Tuning]:** Confirm updating `job_copilot/config.py`, `catalog.py`, `cv_renderer.py`, and `MEMORY.md` with verified `dbt` and `Dagster / Airflow Orchestration` skills once Spec 002 reaches 100% green `pytest`.
