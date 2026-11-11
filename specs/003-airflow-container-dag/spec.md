# Spec 003 — Containerized Apache Airflow DAG Orchestration (Docker + TaskFlow API)

**Status:** approved  
**Author:** Ronald Solares  
**Date:** 2026-11-11  
**Project:** `delta-settlement-lakehouse` (`C:\Users\ronal\delta-settlement-lakehouse`)  

---

## 1. Context and Objective
Spec 002 delivered native Windows/Linux SQL analytics (`dbt-duckdb`) and Dagster Software-Defined Assets (`dagster` + `dagster-dbt`). However, a large share of Data Engineering job postings specifically require **Apache Airflow** (`@dag` / `@task` TaskFlow API, containerized deployment, retry policies, and DAG scheduling).

Because Apache Airflow relies on POSIX-only system modules (`fcntl`, `pwd`) and cannot execute natively inside Windows Python, **Spec 003** introduces a production-style containerized **Apache Airflow** runtime (`Docker Compose` with PostgreSQL metadata backend + custom Airflow image bundling our Medallion Lakehouse & `dbt-duckdb` stack) alongside a refactored shared task execution layer so both Dagster and Airflow share a single source of truth with zero duplicated pipeline logic.

---

## 2. Actors & Target Personas
* **Data Engineer:** Deploys and triggers reproducible Airflow DAGs (`settlement_medallion_dag`) locally in containers using `docker compose`, passing `logical_date` (`ds`) parameters through Bronze, Silver/DLQ, and `dbt build`.
* **CI / Windows Contributor:** Validates task execution, DAG structure, and Docker Compose contracts natively inside `py -m pytest` without requiring Docker-in-Docker in unit tests.

---

## 3. User Stories (Vertical Slices)
* **US-1 (Shared Medallion Task Primitives):** As a Data Engineer, I want pure, framework-agnostic Python task functions (`ingest_bronze_batch`, `run_silver_contract_gate`, `run_dbt_analytics_build`) powering both Dagster assets and Airflow tasks, so business logic lives in one tested place.
* **US-2 (TaskFlow API Airflow DAG):** As a Data Engineer, I want a parameterized Apache Airflow TaskFlow DAG (`settlement_medallion_dag`) executing `bronze_ingest -> silver_contract_gate -> dbt_analytics_build` with `ds` idempotency, retries, and structured XCom summary metrics.
* **US-3 (Containerized Docker Compose Runtime & Live Execution):** As a Platform Engineer, I want a self-contained `Dockerfile.airflow` and `docker-compose.yml` running Apache Airflow (`LocalExecutor` + PostgreSQL) over the project's Delta Lake & DuckDB volumes, verified via an actual live container DAG run (`airflow dags test` / triggered run).

---

## 4. Functional Requirements (EARS Notation)

### Shared Task Layer & Clean Refactoring
* **RF-1:** **THE SYSTEM** extracts Bronze ingestion, Silver/DLQ contract validation with date-scoped Delta cleanup, and `dbt-duckdb` hydration/build into reusable functions inside `delta_settlement_lakehouse/orchestration/tasks.py`, consumed identically by Dagster assets and the Airflow DAG.

### Apache Airflow TaskFlow DAG
* **RF-2:** **WHEN** Apache Airflow parses `airflow/dags/settlement_medallion_dag.py`, **THE SYSTEM** registers `settlement_medallion_dag` with `@daily` schedule, `catchup=False`, retry policy (`retries=2`), and topological dependencies `bronze_ingest >> silver_contract_gate >> dbt_analytics_build`.
* **RF-3:** **WHEN** a DAG run executes for an Airflow logical date (`{{ ds }}` / `batch_date`), **THE SYSTEM** passes `batch_date` cleanly across TaskFlow tasks via XCom metadata and executes idempotent date-scoped writes across Delta Lake and DuckDB marts.

### Containerized Runtime & Verification
* **RF-4:** **WHEN** `docker compose` builds and starts the Airflow stack, **THE SYSTEM** boots PostgreSQL and Airflow cleanly using mounted source/data volumes and non-hardcoded container environment configuration.
* **RF-5:** **WHEN** an operator triggers or tests `settlement_medallion_dag` inside the Airflow container, **THE SYSTEM** executes all three tasks (`bronze_ingest`, `silver_contract_gate`, `dbt_analytics_build`) to `SUCCESS` and populates `data/lakehouse` Delta tables + `analytics.duckdb` Gold marts.

---

## 5. Non-Functional Requirements
* **RNF-1 (Zero-Regression Baseline):** All 26 existing tests in `delta-settlement-lakehouse` (`17` Spec 001 + `9` Spec 002) remain 100% green natively on Windows Python 3.12+.
* **RNF-2 (Security & Clean Repo Boundaries):** Zero hardcoded passwords or tokens committed blindly outside explicit local-dev defaults / `.env.example`; Airflow logs/DB volumes stay ignored in `.gitignore`.
* **RNF-3 (Truthful CV / `job_copilot` Alignment):** `"Apache Airflow"` is added to `job_copilot` (`VERIFIED_SKILLS`, catalog bullets/tags, CV translation, and tests) **only after** live containerized execution passes cleanly.

---

## 6. Edge Cases
1. Empty Silver batch (`chaos_ratio=1.0`, 100% DLQ) triggered inside the Airflow DAG must still succeed cleanly across `dbt_analytics_build` with 0 Silver rows.
2. Re-running `settlement_medallion_dag` for the same `ds` (`batch_date`) inside Airflow must remain idempotent without duplicating Delta Silver rows or Gold reconciliation totals.
3. Importing or testing `delta_settlement_lakehouse` on Windows (`pytest`) must never fail due to `apache-airflow` POSIX imports (`fcntl`/`pwd`).

---

## 7. Out of Scope
* Multi-worker Celery/KubernetesExecutor Helm charts or cloud MWAA/Astronomer deployments (AWS/Terraform deferred to Cloud spec).
* Replacing Dagster (Dagster remains active alongside Airflow for multi-orchestrator versatility).

---

## 8. Open Clarifications (Navigator Approval Required)
* **[NEEDS APPROVAL - Airflow Container Architecture]:** Confirm using **Docker Compose (`postgres:16-alpine` + `apache/airflow:2.10.5-python3.12` with `LocalExecutor`)**, custom-built via `Dockerfile.airflow` so the container shares `delta_settlement_lakehouse` and `dbt_analytics` cleanly without POSIX errors on Windows.
* **[NEEDS APPROVAL - Docker Desktop Daemon Startup]:** `docker-desktop` WSL2 state is currently `Stopped`. Confirm whether you will open **Docker Desktop** now (or authorize starting Docker Desktop from CLI) so we can run the real container build & DAG execution during verification.
* **[NEEDS APPROVAL - `job_copilot` Post-Execution Tuning]:** Confirm adding verified **`Apache Airflow`** skill, updated project description/bullets, and tests in `job_copilot` once container verification succeeds.
