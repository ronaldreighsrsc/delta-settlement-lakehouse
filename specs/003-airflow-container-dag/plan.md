# Implementation Plan — Spec 003: Containerized Apache Airflow DAG Orchestration

**Spec Reference:** [`spec.md`](./spec.md) (`Status: approved`)  
**Author:** Ronald Solares  
**Date:** 2026-11-11  

---

## 1. Architectural Overview & Multi-Orchestrator Clean Design

Instead of duplicating Medallion ingestion, contract validation, or `dbt-duckdb` build code between Dagster and Apache Airflow, **Spec 003** extracts framework-agnostic Python task primitives into `delta_settlement_lakehouse/orchestration/tasks.py`. Both **Dagster** (native Windows/Linux) and **Apache Airflow** (Linux/Docker container) delegate strictly to those shared primitives:

```text
                ┌──► [Dagster Assets] (assets.py — Windows/Linux native)
[Shared Tasks] ─┤
(tasks.py)      └──► [Airflow TaskFlow DAG] (airflow/dags/settlement_medallion_dag.py — Docker Linux)
                        │
                        ├── @task bronze_ingest
                        ├── @task silver_contract_gate  ──► Delta Lake + DuckDB raw_lakehouse
                        └── @task dbt_analytics_build   ──► dbt-duckdb marts + SQL/YAML tests
```

---

## 2. Key Technical Decisions & Constraints

| Decision | Choice | Alternative Rejected | Technical Rationale |
| :--- | :--- | :--- | :--- |
| **Container Topology** | `postgres:16-alpine` + `apache/airflow:2.10.5-python3.12` (`LocalExecutor`) | SQLite `SequentialExecutor` | Matches real-world production compose topology (`airflow-init`, `airflow-webserver`, `airflow-scheduler`, Postgres healthchecks) while remaining lightweight locally. |
| **Python Dependency Isolation inside Image** | Verified in Spike T0 (direct install vs `/opt/lakehouse_venv` fallback if Airflow constraint clash occurs) | Blind unverified `pip install` | `apache/airflow:2.10.5-python3.12` pins strict Airflow constraints (`pydantic`, `SQLAlchemy`, `click`). Spike T0 proves whether `deltalake + duckdb + dbt-duckdb` coexist cleanly or require a subprocess venv. |
| **Shared Pure Task Module** | `orchestration/tasks.py` | Copying Bronze/Silver/dbt code into `dags/` | Enforces DRY architecture and allows full Windows-native `pytest` coverage of the exact Python logic executed by Airflow tasks. |
| **Secret & Runtime Hygiene** | `.env.example` + env interpolation (`${POSTGRES_PASSWORD:-...}`) | Hardcoded production credentials | Complies with `constitution.md` Principle 5 (`Seguridad y Aislamiento`); Airflow logs and `.env` remain excluded in `.gitignore`. |

---

## 3. Repository Structure Additions

```text
delta-settlement-lakehouse/
├── airflow/
│   └── dags/
│       └── settlement_medallion_dag.py   # TaskFlow API (@dag / @task) orchestrator
├── delta_settlement_lakehouse/
│   └── orchestration/
│       ├── tasks.py                      # Shared pure Bronze, Silver/DLQ & dbt task callables
│       └── assets.py                     # Refactored to delegate to orchestration/tasks.py
├── Dockerfile.airflow                    # Custom Python 3.12 Airflow image + Lakehouse deps
├── docker-compose.yml                    # Postgres 16 + Airflow init/webserver/scheduler stack
├── .env.example                          # Documented container env template
├── .gitignore                            # Ignore airflow/logs/, airflow/*.db, .env
└── tests/
    └── test_airflow_dag.py               # Shared task E2E + AST DAG contract + Compose checks
```

---

## 4. Verification Strategy

1. **Spike `T0`:** Confirm Docker daemon is ready (`docker info`), pull/inspect `apache/airflow:2.10.5-python3.12`, and test installing `deltalake`, `duckdb`, `pyarrow`, `polars`, `dbt-core`, `dbt-duckdb`, `typer`, `rich` cleanly inside the container runtime.
2. **TDD Vertical Slices (`T1`–`T4`):**
   - `T1`: Extract `orchestration/tasks.py` & refactor `assets.py` (all existing 26 tests stay green).
   - `T2`: Author `airflow/dags/settlement_medallion_dag.py` (`@dag` / `@task`) + Red/Green tests in `tests/test_airflow_dag.py`.
   - `T3`: Author `Dockerfile.airflow`, `docker-compose.yml`, `.env.example`, `.gitignore` + compose validation tests.
   - `T4`: Live container verification (`docker compose build`, `airflow db migrate`, `airflow dags list`, `airflow dags test settlement_medallion_dag 2026-10-28`, verify live Delta & DuckDB outputs inside `/opt/airflow/project/data/lakehouse`), then tear down cleanly (`docker compose down`).
3. **Docs & Ecosystem Alignment (`T5`):** Update `README.md`, `MEMORY.md`, verify full Windows `py -m pytest tests -q` green, commit & push `delta-settlement-lakehouse`, then tune `job_copilot` (`Apache Airflow` skill + tests).

