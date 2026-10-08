"""Integration tests for the complete Medallion Lakehouse pipeline (Bronze -> Silver/DLQ -> Gold)."""

from datetime import datetime, timezone
from decimal import Decimal
import pytest

from delta_settlement_lakehouse.engines.chaos_generator import SyntheticFeedGenerator
from delta_settlement_lakehouse.engines.contract_validator import DataContractValidator
from delta_settlement_lakehouse.engines.delta_storage import DeltaLakeStorage
from delta_settlement_lakehouse.engines.olap_engine import OlapEngine
from delta_settlement_lakehouse.services.bronze_service import BronzeService
from delta_settlement_lakehouse.services.gold_service import GoldService
from delta_settlement_lakehouse.services.pipeline_runner import PipelineRunner
from delta_settlement_lakehouse.services.silver_service import SilverService


@pytest.fixture
def lakehouse_dir(tmp_path):
    return str(tmp_path / "lakehouse_medallion")


def test_end_to_end_medallion_pipeline(lakehouse_dir):
    storage = DeltaLakeStorage(base_path=lakehouse_dir)
    validator = DataContractValidator()
    olap = OlapEngine()

    bronze_service = BronzeService(storage=storage)
    silver_service = SilverService(storage=storage, validator=validator)
    gold_service = GoldService(storage=storage, olap=olap)

    runner = PipelineRunner(
        bronze_service=bronze_service,
        silver_service=silver_service,
        gold_service=gold_service,
    )

    # 1. Generate 100 events with 10% chaos (anomalous events destined for DLQ)
    gen = SyntheticFeedGenerator(seed=777)
    batch_date = "2026-10-08"
    raw_events = gen.generate_batch(count=100, batch_date=batch_date, inject_chaos=True, chaos_ratio=0.10)

    # 2. Execute end-to-end pipeline
    result = runner.run_pipeline(raw_events=raw_events, batch_date=batch_date)

    # 3. Assertions on pipeline counts
    assert result.bronze_count == 100
    assert result.silver_valid_count > 80
    assert result.dlq_count > 0
    assert result.silver_valid_count + result.dlq_count == 100
    assert result.gold_reconciled_currencies > 0

    # 4. Verify storage tables exist and can be read back
    bronze_table = storage.read_table("bronze_raw_events")
    silver_table = storage.read_table("silver_settlements")
    dlq_table = storage.read_table("silver_quarantine_dlq")
    gold_table = storage.read_table("gold_daily_reconciliation")

    assert bronze_table.num_rows == 100
    assert silver_table.num_rows == result.silver_valid_count
    assert dlq_table.num_rows == result.dlq_count
    assert gold_table.num_rows == result.gold_reconciled_currencies
