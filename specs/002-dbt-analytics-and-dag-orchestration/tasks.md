# Task Breakdown — Spec 002: Modular SQL Analytics Layer & Asset DAG Orchestration

**Plan Reference:** [`plan.md`](./plan.md)  
**Methodology:** Vertical Slice SDD + `/pair` Semáforo TDD (`🔴 Red -> 🟢 Green -> 🔵 Refactor`)

---

## Checklist

- [x] **T0 — Runtime Dependency & Boundary Spike (Mandatory before coding)**
  - Install binary wheels cleanly for `dbt-duckdb`, `dagster`, and `dagster-dbt` in Python 3.12 on Windows; pin clean bounds in `requirements.txt` and add `.gitignore` entries for dbt/Dagster build caches (`dbt_analytics/target/`, `dbt_analytics/logs/`, `.dagster/`).
  - Verify `DeltaLakeStorage` PyArrow schema (`SILVER_SCHEMA` / `DLQ_SCHEMA`) + `empty_table()` hydration into DuckDB + `dbt parse` manifest compilation works offline without external DuckDB extensions.

- [x] **T1 — Vertical Slice 1: `dbt-duckdb` Project, Delta Bridge & Staging Models (`US-1`, `RF-1`)**
  - **🔴 Red:** Write failing test in `tests/test_dbt_analytics.py` expecting `DeltaAnalyticsBridge` to hydrate `raw_lakehouse.silver_settlements` and `raw_lakehouse.silver_quarantine_dlq` from Delta tables (including 0-row empty Silver Edge Case #1) and materialize `stg_settlements` and `stg_quarantine_dlq` views.
  - **🟢 Green:** Create `delta_settlement_lakehouse/engines/dbt_bridge.py`, `dbt_analytics/dbt_project.yml`, `dbt_analytics/profiles.yml`, `dbt_analytics/models/staging/sources.yml`, `stg_settlements.sql`, `stg_quarantine_dlq.sql`, and `schema.yml`.
  - **🔵 Refactor:** Verify idempotency, clean temp-path isolation via `LAKEHOUSE_DUCKDB_PATH`, and `pytest` green.

- [x] **T2 — Vertical Slice 2: Gold Kimball SQL Marts & Declarative Quality Tests (`US-1`, `US-2`, `RF-2`, `RF-3`, `RF-4`)**
  - **🔴 Red:** Add failing tests in `tests/test_dbt_analytics.py` checking: (a) `dim_corridors`, `fct_daily_settlements`, `fct_counterparty_exposure` accurate metrics matching `OlapEngine` 15 bps fee math; (b) `dbt build` generates `manifest.json` lineage; (c) deliberate corruption injected into `raw_lakehouse.silver_settlements` (`settlement_amount = -100` or invalid currency `'XXX'`) fails `dbt test` deterministically with non-zero exit code.
  - **🟢 Green:** Create `dbt_analytics/models/marts/` (`dim_corridors.sql`, `fct_daily_settlements.sql`, `fct_counterparty_exposure.sql`, `schema.yml`) and `dbt_analytics/tests/` (`assert_positive_settled_amounts.sql`, `assert_reconciled_silver_parity.sql`).
  - **🔵 Refactor:** Ensure SQL clarity, schema documentation descriptions, and fast execution (`<5s`).

- [x] **T3 — Vertical Slice 3: Dagster Software-Defined Asset DAG & Runtime Metadata (`US-3`, `RF-5`, `RF-6`)**
  - **🔴 Red:** Write failing integration tests in `tests/test_orchestration_dag.py` executing Dagster `materialize(...)` end-to-end (`bronze_raw_events -> silver_settlements + silver_quarantine_dlq -> @dbt_assets`) verifying structured asset metadata (`row_count`, `dlq_count`, `batch_date`), 100%-DLQ empty-Silver resilience (Edge Case #1), and same-date re-run idempotency (Edge Case #2).
  - **🟢 Green:** Implement `delta_settlement_lakehouse/orchestration/__init__.py`, `assets.py`, `definitions.py` (with date-scoped idempotent Delta overwrite + `dagster-dbt` `DbtCliResource` + `manifest.json` parsing).
  - **🔵 Refactor:** Keep clean separation between orchestration wiring and pure domain/engine logic; verify full pytest suite speed (`RNF-2` `<15s` budget).

- [x] **T4 — Vertical Slice 4: CLI `lakehouse orchestrate` Command & Smoke Verification (`US-3`, `RF-5`)**
  - **🔴 Red:** Add failing CLI smoke test in `tests/test_cli_smoke.py` invoking `py -m delta_settlement_lakehouse.cli orchestrate --date ... --count ... --chaos --base-path ... --duckdb-path ...`.
  - **🟢 Green:** Wire `orchestrate` command into `delta_settlement_lakehouse/cli.py` with Rich formatted status/lineage summary and exit code propagation on dbt test/asset failures.
  - **🔵 Refactor:** Verify help output, parameter defaults, and all CLI tests green.

- [x] **T5 — Documentation, Memory Consolidation & Git Push (`RNF-3`)**
  - Update `README.md` (International Technical English) with Spec 002 Dagster + `dbt-duckdb` Mermaid diagram, commands, test badge count, and directory tree.
  - Update `MEMORY.md` concisely (<100 lines), run full `py -m pytest tests -v` verifying zero regressions (`17 + new Spec 002 tests = 100% pass`), commit cleanly, and push to origin.

