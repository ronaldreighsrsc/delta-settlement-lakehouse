# Task Breakdown — Spec 003: Containerized Apache Airflow DAG Orchestration

**Plan Reference:** [`plan.md`](./plan.md)  
**Methodology:** Vertical Slice SDD + `/pair` Semáforo TDD (`🔴 Red -> 🟢 Green -> 🔵 Refactor`)

---

## Checklist

- [x] **T0 — Docker Daemon & Airflow Image Dependency Spike**
  - Confirm Docker Desktop Linux engine is reachable (`docker info`).
  - Run ephemeral container probe against `apache/airflow:2.10.5-python3.12` testing coexistence of `deltalake`, `duckdb`, `pyarrow`, `polars`, `dbt-core<1.11`, `dbt-duckdb<1.11`, `typer`, `rich` cleanly without breaking `airflow version` or `pip check` (validated isolated `/opt/lakehouse_venv` with zero `pip check` errors on both Airflow base Python and `/opt/lakehouse_venv` because `dbt-core>=1.9` bumps `protobuf>=6.0` vs Airflow `opentelemetry-proto`).

- [x] **T1 — Vertical Slice 1: Shared Medallion Task Layer (`orchestration/tasks.py`) + Dagster Refactor (`US-1`, `RF-1`)**
  - **🔴 Red:** Add shared-task tests in `tests/test_airflow_dag.py` importing `execute_bronze_ingestion`, `execute_silver_contract_gate`, `execute_dbt_analytics_build` from `delta_settlement_lakehouse.orchestration.tasks`.
  - **🟢 Green:** Implement `delta_settlement_lakehouse/orchestration/tasks.py` and refactor `assets.py` to delegate cleanly to those shared functions.
  - **🔵 Refactor:** Verify zero regression across all existing Dagster + Spec 001/002 tests (`26 passed`).

- [x] **T2 — Vertical Slice 2: Apache Airflow TaskFlow DAG (`airflow/dags/settlement_medallion_dag.py`) (`US-2`, `RF-2`, `RF-3`)**
  - **🔴 Red:** Add AST & DAG contract verification tests in `tests/test_airflow_dag.py` asserting `@dag`, `@daily`, `catchup=False`, `retries`, `@task` stage functions (`bronze_ingest`, `silver_contract_gate`, `dbt_analytics_build`), XCom data passing, and Windows import safety.
  - **🟢 Green:** Author `airflow/dags/settlement_medallion_dag.py` using Apache Airflow 2.10 TaskFlow API (`@dag` / `@task`) + `ds` date parameterization + idempotent shared task calls.
  - **🔵 Refactor:** Ensure clean docstrings, typed XCom dicts, and Windows `pytest` green.

- [x] **T3 — Vertical Slice 3: Docker Compose & Custom Image Stack (`US-3`, `RF-4`)**
  - **🔴 Red:** Add YAML & Dockerfile configuration assertions in `tests/test_airflow_dag.py` checking required services (`postgres`, `airflow-init`, `airflow-webserver`, `airflow-scheduler`), healthchecks, mounts, `.env.example`, and `.gitignore` exclusions.
  - **🟢 Green:** Create `Dockerfile.airflow`, `docker-compose.yml`, `.env.example`, and update `.gitignore` (`airflow/logs/`, `.env`, `airflow/*.db`).
  - **🔵 Refactor:** Verify Windows `py -m pytest tests -q` stays 100% green.

- [x] **T4 — Live Container Build & End-to-End Airflow DAG Run (`RF-5`)**
  - Build custom Airflow image via `docker compose build`.
  - Initialize Postgres metadata DB (`docker compose up -d postgres` + `docker compose run --rm airflow-init`), parse DAG (`airflow dags list`), and run real task execution (`airflow dags test settlement_medallion_dag 2026-10-28`) confirming `bronze_ingest`, `silver_contract_gate`, and `dbt_analytics_build` all complete cleanly and write real Delta & DuckDB marts.
  - Verify live scheduler/webserver health briefly if applicable and run `docker compose down` cleanly so no containers leak resources.

- [x] **T5 — Documentation, Git Push & `job_copilot` Alignment (`RNF-1`, `RNF-3`)**
  - Update `README.md` & `MEMORY.md` in `delta-settlement-lakehouse`, run final `py -m pytest tests -q`, commit & push Spec 003.
  - Update `job_copilot` (`config.py`, `domain/catalog.py`, `engines/cv_renderer.py`, `engines/post_generator.py`, `MEMORY.md`, and unit tests) with verified `Apache Airflow` skill, run full `py -m pytest job_copilot/tests -q`, commit & push.
