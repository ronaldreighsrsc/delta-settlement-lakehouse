"""Unit and integration tests for Delta Lake storage layer and time travel engine."""

import os
from datetime import datetime, timezone
from decimal import Decimal
import pyarrow as pa
import pytest

from delta_settlement_lakehouse.domain.models import Currency, SettlementRecord, SettlementStatus
from delta_settlement_lakehouse.engines.delta_storage import DeltaLakeStorage


@pytest.fixture
def temp_delta_dir(tmp_path):
    return str(tmp_path / "lakehouse_test")


def test_delta_storage_write_and_read(temp_delta_dir):
    storage = DeltaLakeStorage(base_path=temp_delta_dir)
    table_name = "test_silver_settlements"

    records = [
        SettlementRecord(
            transaction_id="tx_01",
            batch_id="batch_01",
            timestamp_utc=datetime(2026, 10, 8, 10, 0, tzinfo=timezone.utc),
            sender_bic="CHASUS33XXX",
            receiver_bic="SANESMMXXXX",
            settlement_currency=Currency.USD,
            settlement_amount=Decimal("10000.00"),
            target_currency=Currency.EUR,
            booked_fx_rate=Decimal("0.9250"),
            target_amount=Decimal("9250.00"),
            status=SettlementStatus.SETTLED,
            reconciliation_hash="hash_01",
        ),
        SettlementRecord(
            transaction_id="tx_02",
            batch_id="batch_01",
            timestamp_utc=datetime(2026, 10, 8, 10, 5, tzinfo=timezone.utc),
            sender_bic="CITIUS33XXX",
            receiver_bic="BARCGB22XXX",
            settlement_currency=Currency.EUR,
            settlement_amount=Decimal("20000.00"),
            target_currency=Currency.USD,
            booked_fx_rate=Decimal("1.0800"),
            target_amount=Decimal("21600.00"),
            status=SettlementStatus.SETTLED,
            reconciliation_hash="hash_02",
        ),
    ]

    # Write initial batch (version 0)
    version_0 = storage.write_settlements(table_name, records, mode="append")
    assert version_0 >= 0

    # Read back table
    arrow_table = storage.read_table(table_name)
    assert arrow_table.num_rows == 2
    assert "transaction_id" in arrow_table.column_names

    # Write second batch (version 1)
    record_3 = [
        SettlementRecord(
            transaction_id="tx_03",
            batch_id="batch_02",
            timestamp_utc=datetime(2026, 10, 8, 11, 0, tzinfo=timezone.utc),
            sender_bic="BNPAFRPPXXX",
            receiver_bic="SANESMMXXXX",
            settlement_currency=Currency.USD,
            settlement_amount=Decimal("30000.00"),
            target_currency=Currency.EUR,
            booked_fx_rate=Decimal("0.9250"),
            target_amount=Decimal("27750.00"),
            status=SettlementStatus.SETTLED,
            reconciliation_hash="hash_03",
        )
    ]
    version_1 = storage.write_settlements(table_name, record_3, mode="append")
    assert version_1 > version_0

    # Read latest version (should have 3 rows)
    latest_table = storage.read_table(table_name)
    assert latest_table.num_rows == 3

    # Time Travel: read historical version 0 (must have 2 rows)
    v0_table = storage.read_table(table_name, version=version_0)
    assert v0_table.num_rows == 2


def test_delta_storage_history_and_partitions(temp_delta_dir):
    storage = DeltaLakeStorage(base_path=temp_delta_dir)
    table_name = "test_partitioned"

    records = [
        SettlementRecord(
            transaction_id="tx_p1",
            batch_id="b1",
            timestamp_utc=datetime(2026, 10, 8, 10, 0, tzinfo=timezone.utc),
            sender_bic="CHASUS33XXX",
            receiver_bic="SANESMMXXXX",
            settlement_currency=Currency.CLP,
            settlement_amount=Decimal("5000000.00"),
            target_currency=Currency.USD,
            booked_fx_rate=Decimal("0.00106"),
            target_amount=Decimal("5300.00"),
            status=SettlementStatus.SETTLED,
            reconciliation_hash="hash_p1",
        )
    ]

    storage.write_settlements(table_name, records, mode="append")
    history = storage.get_table_history(table_name)

    assert len(history) >= 1
    assert "version" in history[0]
