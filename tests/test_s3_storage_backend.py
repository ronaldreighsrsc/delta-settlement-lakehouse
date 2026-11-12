"""TDD tests for Spec 004 T1: Dual S3/Local DeltaLakeStorage backend, zone routing, and shared task wiring."""

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from unittest.mock import MagicMock

import pyarrow as pa
import pytest

from delta_settlement_lakehouse.domain.models import (
    Currency,
    RawPaymentEvent,
    SettlementRecord,
    SettlementStatus,
)
from delta_settlement_lakehouse.engines.chaos_generator import SyntheticFeedGenerator
from delta_settlement_lakehouse.engines import dbt_bridge as dbt_bridge_module
from delta_settlement_lakehouse.engines import delta_storage as delta_storage_module
from delta_settlement_lakehouse.engines.dbt_bridge import DeltaAnalyticsBridge
from delta_settlement_lakehouse.engines.delta_storage import (
    DLQ_SCHEMA,
    SILVER_SCHEMA,
    DeltaLakeStorage,
)
from delta_settlement_lakehouse.orchestration import tasks as tasks_module


def _sample_settlement() -> SettlementRecord:
    return SettlementRecord(
        transaction_id="tx_s3_01",
        batch_id="batch_20261028_01",
        timestamp_utc=datetime(2026, 10, 28, 12, 0, tzinfo=timezone.utc),
        sender_bic="CHASUS33XXX",
        receiver_bic="SANESMMXXXX",
        settlement_currency=Currency.USD,
        settlement_amount=Decimal("5000.00"),
        target_currency=Currency.EUR,
        booked_fx_rate=Decimal("0.9200"),
        target_amount=Decimal("4600.00"),
        status=SettlementStatus.SETTLED,
        reconciliation_hash="hash_s3_01",
    )


def _sample_raw_event() -> RawPaymentEvent:
    return RawPaymentEvent(
        event_id="evt_s3_01",
        batch_id="batch_20261028_01",
        ingested_at_utc=datetime(2026, 10, 28, 12, 0, tzinfo=timezone.utc),
        ingestion_date="2026-10-28",
        source_feed="swift_mx",
        raw_payload='{"transaction_id": "tx_s3_01"}',
    )


def test_s3_base_path_detection_does_not_create_local_s3_directory(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    storage = DeltaLakeStorage(
        base_path="s3://settlement-lakehouse/",
        storage_options={"AWS_REGION": "us-east-1"},
    )

    assert storage.is_s3 is True
    assert not (tmp_path / "s3:").exists()
    assert not (tmp_path / "s3").exists()
    assert storage.get_table_uri("bronze_raw_events") == "s3://settlement-lakehouse/bronze_raw_events"
    assert storage._get_table_path("silver_settlements") == "s3://settlement-lakehouse/silver_settlements"


def test_zone_buckets_route_medallion_tables_and_normalize_prefixes(tmp_path, monkeypatch):
    monkeypatch.chdir(tmp_path)
    storage = DeltaLakeStorage(
        base_path="data/lakehouse",
        zone_buckets={
            "bronze": "settlement-bronze-bucket/",
            "silver": "s3://settlement-silver-bucket/",
            "dlq": "s3://settlement-dlq-bucket",
            "gold": "settlement-gold-bucket",
        },
        storage_options={"AWS_REGION": "us-east-1"},
    )

    assert storage.is_s3 is True
    assert storage.get_table_uri("bronze_raw_events") == "s3://settlement-bronze-bucket/bronze_raw_events"
    assert storage.get_table_uri("silver_settlements") == "s3://settlement-silver-bucket/silver_settlements"
    assert storage.get_table_uri("silver_quarantine_dlq") == "s3://settlement-dlq-bucket/silver_quarantine_dlq"
    assert storage.get_table_uri("gold_daily_reconciliation") == "s3://settlement-gold-bucket/gold_daily_reconciliation"
    assert not (tmp_path / "data" / "lakehouse").exists()


def test_from_env_resolves_zone_buckets_and_delta_storage_options(monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_S3_BRONZE_BUCKET", "s3://dev-bronze")
    monkeypatch.setenv("LAKEHOUSE_S3_SILVER_BUCKET", "dev-silver")
    monkeypatch.setenv("LAKEHOUSE_S3_DLQ_BUCKET", "s3://dev-dlq")
    monkeypatch.setenv("LAKEHOUSE_S3_GOLD_BUCKET", "dev-gold")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://s3-mock:5000")
    monkeypatch.setenv("AWS_ACCESS_KEY_ID", "mock_access_key")
    monkeypatch.setenv("AWS_SECRET_ACCESS_KEY", "mock_secret_key")
    monkeypatch.setenv("AWS_DEFAULT_REGION", "us-east-1")
    monkeypatch.setenv("AWS_ALLOW_HTTP", "true")
    monkeypatch.setenv("AWS_S3_ALLOW_UNSAFE_RENAME", "true")

    storage = DeltaLakeStorage.from_env()

    assert storage.is_s3 is True
    assert storage.get_table_uri("bronze_raw_events") == "s3://dev-bronze/bronze_raw_events"
    assert storage.get_table_uri("silver_settlements") == "s3://dev-silver/silver_settlements"
    assert storage.storage_options["AWS_ENDPOINT_URL"] == "http://s3-mock:5000"
    assert storage.storage_options["AWS_ALLOW_HTTP"] == "true"
    assert storage.storage_options["AWS_S3_ALLOW_UNSAFE_RENAME"] == "true"
    assert storage.storage_options["AWS_REGION"] == "us-east-1"


def test_s3_operations_forward_storage_options_to_delta_rs(monkeypatch):
    calls: dict[str, list] = {"write": [], "dt": [], "exists": [], "delete": []}
    s3_opts = {
        "AWS_ENDPOINT_URL": "http://localhost:15000",
        "AWS_ACCESS_KEY_ID": "mock_key",
        "AWS_SECRET_ACCESS_KEY": "mock_secret",
        "AWS_ALLOW_HTTP": "true",
        "AWS_S3_ALLOW_UNSAFE_RENAME": "true",
    }

    class FakeDeltaTable:
        def __init__(self, table_uri, version=None, storage_options=None):
            calls["dt"].append((table_uri, version, storage_options))

        @staticmethod
        def is_deltatable(table_uri, storage_options=None):
            calls["exists"].append((table_uri, storage_options))
            return True

        def version(self):
            return 3

        def to_pyarrow_table(self):
            return pa.Table.from_pylist([], schema=SILVER_SCHEMA)

        def history(self):
            return [{"version": 3, "operation": "WRITE"}]

        def delete(self, predicate):
            calls["delete"].append(predicate)

    def fake_write(dest, table, mode="append", partition_by=None, storage_options=None):
        calls["write"].append((dest, mode, partition_by, storage_options))

    monkeypatch.setattr(delta_storage_module, "DeltaTable", FakeDeltaTable)
    monkeypatch.setattr(delta_storage_module, "write_deltalake", fake_write)

    storage = DeltaLakeStorage(
        base_path="s3://bronze-bucket",
        zone_buckets={"bronze": "s3://bronze-bucket", "silver": "s3://silver-bucket"},
        storage_options=s3_opts,
    )

    assert storage.write_raw_events("bronze_raw_events", [_sample_raw_event()]) == 3
    assert storage.table_exists("silver_settlements") is True
    assert storage.delete_where("silver_settlements", "settlement_date = '2026-10-28'") is True
    assert storage.read_table("silver_settlements", version=1).schema == SILVER_SCHEMA
    assert storage.get_table_history("silver_settlements") == [{"version": 3, "operation": "WRITE"}]

    assert calls["write"][0][0] == "s3://bronze-bucket/bronze_raw_events"
    assert calls["write"][0][3] == s3_opts
    assert calls["exists"][0] == ("s3://silver-bucket/silver_settlements", s3_opts)
    assert calls["delete"] == ["settlement_date = '2026-10-28'"]
    assert ("s3://silver-bucket/silver_settlements", 1, s3_opts) in calls["dt"]
    assert len(calls["dt"]) == 4


def test_local_explicit_path_ignores_s3_env_vars(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_S3_BRONZE_BUCKET", "s3://should-not-hit")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://should-not-hit:5000")

    local_dir = tmp_path / "local_lakehouse"
    storage = DeltaLakeStorage(base_path=str(local_dir))

    assert storage.is_s3 is False
    assert storage.table_exists("bronze_raw_events") is False
    ver = storage.write_raw_events("bronze_raw_events", [_sample_raw_event()])
    assert ver == 0
    assert storage.table_exists("bronze_raw_events") is True
    assert storage.delete_where("bronze_raw_events", "ingestion_date = '2026-10-28'") is True


def test_tasks_and_bridge_respect_s3_env_defaults_and_empty_silver(tmp_path, monkeypatch):
    captured_storages: list[DeltaLakeStorage] = []
    duckdb_path = tmp_path / "analytics.duckdb"

    monkeypatch.setenv("LAKEHOUSE_S3_BRONZE_BUCKET", "s3://zone-bronze")
    monkeypatch.setenv("LAKEHOUSE_S3_SILVER_BUCKET", "s3://zone-silver")
    monkeypatch.setenv("LAKEHOUSE_S3_DLQ_BUCKET", "s3://zone-dlq")
    monkeypatch.setenv("LAKEHOUSE_S3_GOLD_BUCKET", "s3://zone-gold")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://s3-mock:5000")

    class FakeStorage(DeltaLakeStorage):
        def __init__(self, *args, **kwargs):
            super().__init__(*args, **kwargs)
            captured_storages.append(self)

        def table_exists(self, table_name: str) -> bool:
            return table_name == "silver_quarantine_dlq"

        def read_table(self, table_name: str, version=None, timestamp=None):
            if table_name == "silver_quarantine_dlq":
                return pa.Table.from_pylist(
                    [
                        {
                            "event_id": "evt_bad",
                            "batch_id": "batch_20261028_01",
                            "quarantined_at_utc": datetime(2026, 10, 28, tzinfo=timezone.utc),
                            "rejection_reason": "ERR_INVALID_CURRENCY",
                            "raw_payload": "{}",
                        }
                    ],
                    schema=DLQ_SCHEMA,
                )
            return SILVER_SCHEMA.empty_table()

    monkeypatch.setattr(dbt_bridge_module, "DeltaLakeStorage", FakeStorage)
    monkeypatch.setattr(tasks_module, "DeltaLakeStorage", FakeStorage)

    bridge = DeltaAnalyticsBridge()
    counts = bridge.hydrate_sources(
        lakehouse_base_path=dbt_bridge_module.DEFAULT_LAKEHOUSE_BASE_PATH,
        duckdb_path=duckdb_path,
    )
    assert counts["silver_settlements"] == 0
    assert counts["silver_quarantine_dlq"] == 1
    assert captured_storages[0].get_table_uri("silver_quarantine_dlq") == "s3://zone-dlq/silver_quarantine_dlq"
    assert captured_storages[0].storage_options["AWS_ENDPOINT_URL"] == "http://s3-mock:5000"


def test_shared_tasks_route_to_s3_zone_buckets_when_env_configured(tmp_path, monkeypatch):
    monkeypatch.setenv("LAKEHOUSE_S3_BRONZE_BUCKET", "s3://zone-bronze")
    monkeypatch.setenv("LAKEHOUSE_S3_SILVER_BUCKET", "s3://zone-silver")
    monkeypatch.setenv("LAKEHOUSE_S3_DLQ_BUCKET", "s3://zone-dlq")
    monkeypatch.setenv("LAKEHOUSE_S3_GOLD_BUCKET", "s3://zone-gold")
    monkeypatch.setenv("AWS_ENDPOINT_URL", "http://s3-mock:5000")

    deleted_predicates: list[tuple[str, str]] = []
    written_uris: list[str] = []

    sample_event = SyntheticFeedGenerator(seed=42).generate_batch(
        count=1,
        batch_date="2026-10-28",
        inject_chaos=False,
    )[0]

    class FakeTaskStorage(DeltaLakeStorage):
        def table_exists(self, table_name: str) -> bool:
            return table_name == "bronze_raw_events"

        def delete_where(self, table_name: str, predicate: str) -> bool:
            deleted_predicates.append((table_name, predicate))
            return True

        def write_raw_events(self, table_name: str, events, mode: str = "append") -> int:
            written_uris.append(self.get_table_uri(table_name))
            return 0

        def read_table(self, table_name: str, version=None, timestamp=None):
            return pa.Table.from_pylist(
                [
                    {
                        "event_id": sample_event.event_id,
                        "batch_id": sample_event.batch_id,
                        "ingested_at_utc": sample_event.ingested_at_utc,
                        "ingestion_date": sample_event.ingestion_date,
                        "source_feed": sample_event.source_feed,
                        "raw_payload": sample_event.raw_payload,
                    }
                ]
            )

        def write_settlements(self, table_name: str, records, mode: str = "append") -> int:
            written_uris.append(self.get_table_uri(table_name))
            return 0

        def write_quarantine(self, table_name: str, records, mode: str = "append") -> int:
            written_uris.append(self.get_table_uri(table_name))
            return 0

    monkeypatch.setattr(tasks_module, "DeltaLakeStorage", FakeTaskStorage)
    monkeypatch.setattr(dbt_bridge_module, "DeltaLakeStorage", FakeTaskStorage)

    bronze_res = tasks_module.execute_bronze_ingestion(batch_date="2026-10-28", count=3)
    assert bronze_res.row_count == 3

    silver_res = tasks_module.execute_silver_contract_gate(
        duckdb_path=tmp_path / "analytics.duckdb",
        batch_date="2026-10-28",
    )
    assert silver_res.silver_count == 1
    assert silver_res.dlq_count == 0
    assert ("bronze_raw_events", "ingestion_date = '2026-10-28'") in deleted_predicates
    assert "s3://zone-bronze/bronze_raw_events" in written_uris
    assert "s3://zone-silver/silver_settlements" in written_uris

