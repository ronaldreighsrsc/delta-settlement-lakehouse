"""End-to-end integration tests for Dagster + dagster-dbt asset orchestration."""

from pathlib import Path
from dagster import AssetKey
import duckdb
import pytest

from delta_settlement_lakehouse.engines.delta_storage import DeltaLakeStorage
from delta_settlement_lakehouse.orchestration.definitions import (
    build_definitions,
    run_orchestrated_pipeline,
)


@pytest.fixture(scope="module")
def dag_duckdb_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("dag_orchestration_duckdb") / "analytics.duckdb"


@pytest.fixture
def isolated_dag_env(tmp_path: Path, dag_duckdb_path: Path):
    base_path = tmp_path / "lakehouse"
    dag_duckdb_path.unlink(missing_ok=True)
    yield str(base_path), dag_duckdb_path
    dag_duckdb_path.unlink(missing_ok=True)


def test_dagster_asset_graph_topology():
    """Verify upstream Delta assets wire seamlessly into downstream @dbt_assets keys."""
    defs = build_definitions()
    all_keys = set(defs.resolve_asset_graph().get_all_asset_keys())

    expected_keys = {
        AssetKey(["bronze_raw_events"]),
        AssetKey(["raw_lakehouse", "silver_settlements"]),
        AssetKey(["raw_lakehouse", "silver_quarantine_dlq"]),
        AssetKey(["staging", "stg_settlements"]),
        AssetKey(["staging", "stg_quarantine_dlq"]),
        AssetKey(["marts", "dim_corridors"]),
        AssetKey(["marts", "fct_daily_settlements"]),
        AssetKey(["marts", "fct_counterparty_exposure"]),
    }
    assert expected_keys.issubset(all_keys)


def test_orchestrated_dag_end_to_end_metadata_and_idempotency(isolated_dag_env):
    """RF-5, RF-6 & Edge Case 2: Materializes full DAG, emits metadata, and stays idempotent per batch_date."""
    base_path, duckdb_path = isolated_dag_env

    first_run = run_orchestrated_pipeline(
        lakehouse_base_path=base_path,
        duckdb_path=duckdb_path,
        batch_date="2026-10-20",
        count=40,
        inject_chaos=True,
        chaos_ratio=0.10,
        seed=42,
    )
    assert first_run.success
    assert first_run.bronze_count == 40
    assert first_run.silver_count > 30
    assert first_run.dlq_count > 0
    assert first_run.silver_count + first_run.dlq_count == 40
    assert first_run.asset_metadata["bronze_raw_events"]["batch_date"] == "2026-10-20"
    assert first_run.asset_metadata["silver_settlements"]["row_count"] == first_run.silver_count
    assert first_run.asset_metadata["silver_quarantine_dlq"]["dlq_count"] == first_run.dlq_count

    # Edge Case 2: Re-running exact same batch_date must be idempotent without duplicate rows
    rerun = run_orchestrated_pipeline(
        lakehouse_base_path=base_path,
        duckdb_path=duckdb_path,
        batch_date="2026-10-20",
        count=40,
        inject_chaos=True,
        chaos_ratio=0.10,
        seed=42,
    )
    assert rerun.success
    assert rerun.silver_count == first_run.silver_count

    # Run a second distinct date to verify partition isolation
    second_day = run_orchestrated_pipeline(
        lakehouse_base_path=base_path,
        duckdb_path=duckdb_path,
        batch_date="2026-10-21",
        count=20,
        inject_chaos=False,
        seed=99,
    )
    assert second_day.success
    assert second_day.silver_count == 20

    storage = DeltaLakeStorage(base_path=base_path)
    assert storage.read_table("bronze_raw_events").num_rows == 60
    assert storage.read_table("silver_settlements").num_rows == first_run.silver_count + 20

    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        total_mart_tx = conn.execute(
            "SELECT SUM(transaction_count) FROM main_marts.fct_daily_settlements"
        ).fetchone()[0]
        distinct_dates = conn.execute(
            "SELECT COUNT(DISTINCT settlement_date) FROM main_marts.fct_daily_settlements"
        ).fetchone()[0]
    finally:
        conn.close()

    assert total_mart_tx == first_run.silver_count + 20
    assert distinct_dates == 2


def test_orchestrated_dag_handles_empty_silver_batch(isolated_dag_env):
    """Edge Case 1 inside full Dagster DAG: 100% DLQ batch completes with 0 Silver rows safely."""
    base_path, duckdb_path = isolated_dag_env

    res = run_orchestrated_pipeline(
        lakehouse_base_path=base_path,
        duckdb_path=duckdb_path,
        batch_date="2026-10-22",
        count=10,
        inject_chaos=True,
        chaos_ratio=1.0,
        seed=7,
    )
    assert res.success
    assert res.bronze_count == 10
    assert res.silver_count == 0
    assert res.dlq_count == 10

    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        assert conn.execute("SELECT COUNT(*) FROM main_marts.fct_daily_settlements").fetchone()[0] == 0
    finally:
        conn.close()

