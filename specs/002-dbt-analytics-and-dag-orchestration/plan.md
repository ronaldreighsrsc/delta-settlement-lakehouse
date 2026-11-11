# Implementation Plan — Spec 002: Modular SQL Analytics Layer & Asset DAG Orchestration

**Spec Reference:** [`spec.md`](./spec.md) (`Status: approved`)  
**Author:** Ronald Solares  
**Date:** 2026-11-11  

---

## 1. Architectural Overview & Data Flow

Spec 002 layers declarative SQL transformations (`dbt-duckdb`) and Software-Defined Asset orchestration (`dagster` + `dagster-dbt`) cleanly on top of Spec 001's zero-JVM Delta Lake Medallion architecture without breaking any existing API or test.

```text
SyntheticFeedGenerator (seed / batch_date)
        │
        ▼
[Dagster Asset: bronze_raw_events] ──(Delta Lake ACID partition overwrite/append)──► data/lakehouse/bronze_raw_events
        │
        ▼
[Dagster Asset: silver_settlements & silver_quarantine_dlq] ──(DataContractValidator)──► Delta Silver + DLQ
        │
        ▼
[DeltaAnalyticsBridge (dbt_bridge.py)] ──(Reads _delta_log via DeltaLakeStorage -> hydrates DuckDB raw_lakehouse schema)──► analytics.duckdb
        │
        ▼
[Dagster @dbt_assets: dbt build]
  ├── Staging (views): stg_settlements, stg_quarantine_dlq
  ├── Gold Marts (tables): dim_corridors, fct_daily_settlements, fct_counterparty_exposure
  └── Tests: YAML (not_null, unique, accepted_values) + Singular SQL financial invariants
```

---

## 2. Key Technical Decisions & Guardrails

| Decision | Choice | Rejected Alternative | Technical Rationale |
| :--- | :--- | :--- | :--- |
| **Orchestrator** | `Dagster` + `dagster-dbt` | Apache Airflow | Airflow depends on POSIX `fcntl`/`pwd` and fails natively on Windows without Docker/WSL2 (`RNF-1`). Dagster runs natively on Windows Python 3.12+ and Linux CI, compiles in-memory via `materialize(...)` for fast pytest execution (`RNF-2`), and natively renders dbt lineage via `dagster dev`. |
| **Delta -> dbt Bridge** | `DeltaAnalyticsBridge` hydrating `raw_lakehouse.*` tables from `DeltaLakeStorage.read_table()` | DuckDB `INSTALL delta` HTTP extension | Reading Delta logs via `deltalake` (`delta-rs`) respects `_delta_log` ACID commits, works 100% offline/airgapped in CI without external extension downloads, and safely injects typed empty Arrow tables (`SILVER_SCHEMA.empty_table()`) when Silver or DLQ has 0 rows (Edge Case #1). |
| **Profile Path Isolation** | `env_var('LAKEHOUSE_DUCKDB_PATH', ...)` in `dbt_analytics/profiles.yml` | Hardcoded local `.duckdb` path | Guarantees each Pytest test runs against an isolated `tmp_path` DuckDB instance with zero cross-test pollution or accidental mutation of `data/`. |
| **Date-Scoped Idempotency** | Partition/date predicate cleanup on Bronze/Silver during orchestrated batch runs (`delete` by date before write) + `materialized='table'` Gold marts | Pure blind `mode="append"` | Ensures re-running `lakehouse orchestrate --date YYYY-MM-DD` preserves other settlement dates while keeping `silver_settlements` and Gold mart totals identically reconciled (Edge Case #2, `RF-3`). |
| **Binary Wheel Constraint** | Resolve Windows binary wheels cleanly (`--only-binary :all:` compatibility) | Unpinned source builds | Avoids compiler/Cargo prerequisites on Windows contributor machines. |

---

## 3. Repository Structure Additions

```text
delta-settlement-lakehouse/
├── dbt_analytics/
│   ├── dbt_project.yml
│   ├── profiles.yml
│   ├── models/
│   │   ├── staging/
│   │   │   ├── sources.yml
│   │   │   ├── schema.yml
│   │   │   ├── stg_settlements.sql
│   │   │   └── stg_quarantine_dlq.sql
│   │   └── marts/
│   │       ├── schema.yml
│   │       ├── dim_corridors.sql
│   │       ├── fct_daily_settlements.sql
│   │       └── fct_counterparty_exposure.sql
│   └── tests/
│       ├── assert_positive_settled_amounts.sql
│       └── assert_reconciled_silver_parity.sql
├── delta_settlement_lakehouse/
│   ├── engines/
│   │   └── dbt_bridge.py         # Hydrates DuckDB raw_lakehouse schema + runs dbt subprocess/CLI safely
│   ├── orchestration/
│   │   ├── __init__.py
│   │   ├── assets.py             # Bronze, Silver/DLQ, and @dbt_assets + runtime metadata
│   │   └── definitions.py        # Dagster Definitions & DbtCliResource factory
│   └── cli.py                    # New `orchestrate` Typer command with Rich summary
└── tests/
    ├── test_dbt_analytics.py     # Staging, marts, empty-Silver edge case, and corruption test gate
    └── test_orchestration_dag.py # End-to-end Dagster materialize(), metadata, and idempotency tests
```

---

## 4. Analytical Models & Data Quality Contracts

1. **`raw_lakehouse` Sources (`models/staging/sources.yml`):**
   * `silver_settlements` (plus source freshness/schema metadata)
   * `silver_quarantine_dlq`
2. **Staging (`materialized: view`):**
   * `stg_settlements`: Typed projection over `silver_settlements` deriving `corridor_id` (`settlement_currency || '-' || target_currency`) and `estimated_fee_amount` (`ROUND(settlement_amount * 0.0015, 4)` matching Spec 001 `OlapEngine` 15 bps rule).
   * `stg_quarantine_dlq`: Extracts canonical `error_code` (`split_part(rejection_reason, ':', 1)`) for governance trend analytics.
3. **Gold Dimensional Marts (`materialized: table`):**
   * `dim_corridors`: Distinct currency corridors (`corridor_id`, `settlement_currency`, `target_currency`, transaction volume tier metrics).
   * `fct_daily_settlements`: Grain `(settlement_date, settlement_currency)` with `total_settled_amount`, `transaction_count`, `total_fees_collected`, `avg_fx_rate`, `min_settled_amount`, `max_settled_amount`.
   * `fct_counterparty_exposure`: Grain `(sender_bic, receiver_bic, settlement_currency)` with bilateral volume, transaction count, and average ticket size.
4. **Automated Tests (`RF-3`, `RF-4`):**
   * YAML Schema Tests: `not_null` & `unique` on `transaction_id`, `corridor_id`, `event_id`; `accepted_values` on currencies (ISO 4217 `SUPPORTED_CURRENCIES`), settlement statuses (`PENDING`, `SETTLED`, `FAILED`, `QUARANTINED`), and DLQ `error_code`.
   * Singular Invariants: `assert_positive_settled_amounts.sql` (`settlement_amount <= 0` returns failing rows) and `assert_reconciled_silver_parity.sql` (`fct_daily_settlements` totals reconcile strictly against `stg_settlements` per date & currency).

---

## 5. Verification Strategy (/pair Semáforo)

1. **Spike `T0`:** Validate live `dbt-duckdb`, `dagster`, `dagster-dbt` binary installation on Windows Python 3.12+, verify `dbt parse` / manifest generation, and prove PyArrow -> DuckDB schema hydration works cleanly.
2. **Vertical Slices (`T1`–`T5`):** Strict `Red Test -> Minimal Green Implementation -> Refactor` cycle on each slice (`max 3 files` per step where feasible), preserving `17/17` baseline tests + `<15s` execution budget.

