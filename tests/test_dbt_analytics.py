"""Vertical-slice integration tests for the dbt-duckdb SQL analytics & data quality layer."""

from pathlib import Path
import duckdb
import pytest

from delta_settlement_lakehouse.engines.chaos_generator import SyntheticFeedGenerator
from delta_settlement_lakehouse.engines.contract_validator import DataContractValidator
from delta_settlement_lakehouse.engines.dbt_bridge import DeltaAnalyticsBridge
from delta_settlement_lakehouse.engines.delta_storage import DeltaLakeStorage
from delta_settlement_lakehouse.services.silver_service import SilverService


@pytest.fixture(scope="module")
def module_duckdb_path(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("dbt_analytics_session") / "analytics.duckdb"


@pytest.fixture
def isolated_analytics_env(tmp_path: Path, module_duckdb_path: Path):
    base_path = tmp_path / "lakehouse"
    module_duckdb_path.unlink(missing_ok=True)
    yield str(base_path), module_duckdb_path
    module_duckdb_path.unlink(missing_ok=True)


def test_hydrate_and_build_staging_models(isolated_analytics_env):
    base_path, duckdb_path = isolated_analytics_env
    storage = DeltaLakeStorage(base_path=base_path)
    silver_service = SilverService(storage=storage, validator=DataContractValidator())

    events = SyntheticFeedGenerator(seed=101).generate_batch(
        count=40, batch_date="2026-10-12", inject_chaos=True, chaos_ratio=0.15
    )
    valid_records, dlq_records = silver_service.process_events(events)
    assert len(valid_records) > 0
    assert len(dlq_records) > 0

    bridge = DeltaAnalyticsBridge()
    counts = bridge.hydrate_sources(lakehouse_base_path=base_path, duckdb_path=duckdb_path)
    assert counts["silver_settlements"] == len(valid_records)
    assert counts["silver_quarantine_dlq"] == len(dlq_records)

    result = bridge.run_dbt(
        args=["run", "--select", "staging"],
        duckdb_path=duckdb_path,
    )
    assert result.success, result.error_message

    with duckdb.connect(str(duckdb_path), read_only=True) as conn:
        stg_silver_count = conn.execute("SELECT COUNT(*) FROM main_staging.stg_settlements").fetchone()[0]
        stg_dlq_count = conn.execute("SELECT COUNT(*) FROM main_staging.stg_quarantine_dlq").fetchone()[0]
        sample_corridor = conn.execute(
            "SELECT corridor_id, estimated_fee_amount FROM main_staging.stg_settlements LIMIT 1"
        ).fetchone()
        sample_err = conn.execute(
            "SELECT error_code FROM main_staging.stg_quarantine_dlq LIMIT 1"
        ).fetchone()

    assert stg_silver_count == len(valid_records)
    assert stg_dlq_count == len(dlq_records)
    assert "-" in sample_corridor[0]
    assert sample_corridor[1] > 0
    assert sample_err[0].startswith("ERR_")


def test_empty_silver_batch_hydrates_safely_for_staging(isolated_analytics_env):
    """Edge Case 1: 100% DLQ (no Silver table written) must still yield valid 0-row staging views."""
    base_path, duckdb_path = isolated_analytics_env
    storage = DeltaLakeStorage(base_path=base_path)
    silver_service = SilverService(storage=storage, validator=DataContractValidator())

    events = SyntheticFeedGenerator(seed=202).generate_batch(
        count=10, batch_date="2026-10-13", inject_chaos=True, chaos_ratio=1.0
    )
    valid_records, dlq_records = silver_service.process_events(events)
    assert len(valid_records) == 0
    assert len(dlq_records) == 10

    bridge = DeltaAnalyticsBridge()
    counts = bridge.hydrate_sources(lakehouse_base_path=base_path, duckdb_path=duckdb_path)
    assert counts["silver_settlements"] == 0
    assert counts["silver_quarantine_dlq"] == 10

    result = bridge.run_dbt(args=["run", "--select", "staging"], duckdb_path=duckdb_path)
    assert result.success, result.error_message

    with duckdb.connect(str(duckdb_path), read_only=True) as conn:
        assert conn.execute("SELECT COUNT(*) FROM main_staging.stg_settlements").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM main_staging.stg_quarantine_dlq").fetchone()[0] == 10


def test_gold_marts_and_quality_tests_pass(isolated_analytics_env):
    """US-1 & US-2: Full dbt build materializes Gold Kimball marts and passes all schema/invariant tests."""
    base_path, duckdb_path = isolated_analytics_env
    storage = DeltaLakeStorage(base_path=base_path)
    silver_service = SilverService(storage=storage, validator=DataContractValidator())

    events = SyntheticFeedGenerator(seed=303).generate_batch(
        count=60, batch_date="2026-10-14", inject_chaos=True, chaos_ratio=0.10
    )
    valid_records, _ = silver_service.process_events(events)
    assert len(valid_records) > 40

    bridge = DeltaAnalyticsBridge()
    bridge.hydrate_sources(lakehouse_base_path=base_path, duckdb_path=duckdb_path)

    build_res = bridge.run_dbt(args=["build"], duckdb_path=duckdb_path)
    assert build_res.success, build_res.error_message

    manifest_path = bridge.project_dir / "target" / "manifest.json"
    assert manifest_path.exists()

    with duckdb.connect(str(duckdb_path), read_only=True) as conn:
        mart_counts = conn.execute(
            "SELECT SUM(transaction_count), SUM(total_fees_collected) FROM main_marts.fct_daily_settlements"
        ).fetchone()
        corridors_count = conn.execute("SELECT COUNT(*) FROM main_marts.dim_corridors").fetchone()[0]
        exposure_count = conn.execute("SELECT COUNT(*) FROM main_marts.fct_counterparty_exposure").fetchone()[0]

    assert mart_counts[0] == len(valid_records)
    assert mart_counts[1] > 0
    assert corridors_count > 0
    assert exposure_count > 0


def test_empty_silver_batch_builds_empty_gold_marts_cleanly(isolated_analytics_env):
    """Edge Case 1 extended: full dbt build over 0 Silver rows completes cleanly with all tests green."""
    base_path, duckdb_path = isolated_analytics_env
    storage = DeltaLakeStorage(base_path=base_path)
    silver_service = SilverService(storage=storage, validator=DataContractValidator())

    events = SyntheticFeedGenerator(seed=404).generate_batch(
        count=8, batch_date="2026-10-15", inject_chaos=True, chaos_ratio=1.0
    )
    valid_records, dlq_records = silver_service.process_events(events)
    assert len(valid_records) == 0
    assert len(dlq_records) == 8

    bridge = DeltaAnalyticsBridge()
    bridge.hydrate_sources(lakehouse_base_path=base_path, duckdb_path=duckdb_path)
    build_res = bridge.run_dbt(args=["build"], duckdb_path=duckdb_path)
    assert build_res.success, build_res.error_message

    with duckdb.connect(str(duckdb_path), read_only=True) as conn:
        assert conn.execute("SELECT COUNT(*) FROM main_marts.fct_daily_settlements").fetchone()[0] == 0
        assert conn.execute("SELECT COUNT(*) FROM main_marts.dim_corridors").fetchone()[0] == 0


def test_dbt_test_fails_deterministically_on_corrupted_data(isolated_analytics_env):
    """Edge Case 3 & RF-4: Negative settlement amount or invalid ISO currency fails dbt build & halts downstream marts."""
    base_path, duckdb_path = isolated_analytics_env
    storage = DeltaLakeStorage(base_path=base_path)
    silver_service = SilverService(storage=storage, validator=DataContractValidator())

    events = SyntheticFeedGenerator(seed=505).generate_batch(count=15, batch_date="2026-10-16", inject_chaos=False)
    silver_service.process_events(events)

    bridge = DeltaAnalyticsBridge()
    bridge.hydrate_sources(lakehouse_base_path=base_path, duckdb_path=duckdb_path)

    # Inject deliberate corruption directly into hydrated source table BEFORE dbt build
    conn = duckdb.connect(str(duckdb_path))
    try:
        conn.execute(
            """
            UPDATE raw_lakehouse.silver_settlements
            SET settlement_amount = -250.00, settlement_currency = 'XXX'
            WHERE transaction_id = (SELECT MIN(transaction_id) FROM raw_lakehouse.silver_settlements)
            """
        )
    finally:
        conn.close()

    corrupted_build = bridge.run_dbt(args=["build"], duckdb_path=duckdb_path)
    assert not corrupted_build.success
    assert corrupted_build.error_message



