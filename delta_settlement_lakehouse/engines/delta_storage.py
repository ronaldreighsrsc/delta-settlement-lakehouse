"""Delta Lake Storage and Time Travel Engine via delta-rs and PyArrow."""

from datetime import datetime, timezone
from decimal import Decimal
from pathlib import Path
from typing import Any, Optional, Union
from deltalake import DeltaTable, write_deltalake
import pyarrow as pa

from delta_settlement_lakehouse.domain.models import (
    DailyReconciliationSummary,
    QuarantineRecord,
    RawPaymentEvent,
    SettlementRecord,
)

BRONZE_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("batch_id", pa.string()),
    ("ingested_at_utc", pa.timestamp("us", tz="UTC")),
    ("ingestion_date", pa.string()),
    ("source_feed", pa.string()),
    ("raw_payload", pa.string()),
])

SILVER_SCHEMA = pa.schema([
    ("transaction_id", pa.string()),
    ("batch_id", pa.string()),
    ("timestamp_utc", pa.timestamp("us", tz="UTC")),
    ("sender_bic", pa.string()),
    ("receiver_bic", pa.string()),
    ("settlement_currency", pa.string()),
    ("settlement_amount", pa.float64()),
    ("target_currency", pa.string()),
    ("booked_fx_rate", pa.float64()),
    ("target_amount", pa.float64()),
    ("status", pa.string()),
    ("reconciliation_hash", pa.string()),
    ("settlement_year_month", pa.string()),
    ("settlement_date", pa.string()),
])

DLQ_SCHEMA = pa.schema([
    ("event_id", pa.string()),
    ("batch_id", pa.string()),
    ("quarantined_at_utc", pa.timestamp("us", tz="UTC")),
    ("rejection_reason", pa.string()),
    ("raw_payload", pa.string()),
])

GOLD_RECON_SCHEMA = pa.schema([
    ("settlement_date", pa.string()),
    ("currency", pa.string()),
    ("total_settled_amount", pa.float64()),
    ("transaction_count", pa.int64()),
    ("total_fees_collected", pa.float64()),
    ("avg_fx_rate", pa.float64()),
    ("computed_at_utc", pa.timestamp("us", tz="UTC")),
])


class DeltaLakeStorage:
    """Encapsulates ACID reads, atomic writes, partitioning, and time travel over Delta tables."""

    def __init__(self, base_path: Union[str, Path] = "data/lakehouse"):
        self.base_path = Path(base_path)
        self.base_path.mkdir(parents=True, exist_ok=True)

    def _get_table_path(self, table_name: str) -> str:
        p = self.base_path / table_name
        return str(p.resolve())

    def write_raw_events(
        self,
        table_name: str,
        events: list[RawPaymentEvent],
        mode: str = "append",
    ) -> int:
        if not events:
            return -1

        rows = [
            {
                "event_id": e.event_id,
                "batch_id": e.batch_id,
                "ingested_at_utc": e.ingested_at_utc,
                "ingestion_date": e.ingestion_date,
                "source_feed": e.source_feed,
                "raw_payload": e.raw_payload,
            }
            for e in events
        ]
        table = pa.Table.from_pylist(rows, schema=BRONZE_SCHEMA)
        dest = self._get_table_path(table_name)
        write_deltalake(dest, table, mode=mode, partition_by=["ingestion_date"])
        return DeltaTable(dest).version()

    def write_settlements(
        self,
        table_name: str,
        records: list[SettlementRecord],
        mode: str = "append",
    ) -> int:
        if not records:
            return -1

        rows = [
            {
                "transaction_id": r.transaction_id,
                "batch_id": r.batch_id,
                "timestamp_utc": r.timestamp_utc,
                "sender_bic": r.sender_bic,
                "receiver_bic": r.receiver_bic,
                "settlement_currency": r.settlement_currency.value,
                "settlement_amount": float(r.settlement_amount),
                "target_currency": r.target_currency.value,
                "booked_fx_rate": float(r.booked_fx_rate),
                "target_amount": float(r.target_amount),
                "status": r.status.value,
                "reconciliation_hash": r.reconciliation_hash,
                "settlement_year_month": r.settlement_year_month,
                "settlement_date": r.settlement_date,
            }
            for r in records
        ]
        table = pa.Table.from_pylist(rows, schema=SILVER_SCHEMA)
        dest = self._get_table_path(table_name)
        write_deltalake(dest, table, mode=mode, partition_by=["settlement_currency", "settlement_year_month"])
        return DeltaTable(dest).version()

    def write_quarantine(
        self,
        table_name: str,
        records: list[QuarantineRecord],
        mode: str = "append",
    ) -> int:
        if not records:
            return -1

        rows = [
            {
                "event_id": r.event_id,
                "batch_id": r.batch_id,
                "quarantined_at_utc": r.quarantined_at_utc,
                "rejection_reason": r.rejection_reason,
                "raw_payload": r.raw_payload,
            }
            for r in records
        ]
        table = pa.Table.from_pylist(rows, schema=DLQ_SCHEMA)
        dest = self._get_table_path(table_name)
        write_deltalake(dest, table, mode=mode)
        return DeltaTable(dest).version()

    def write_reconciliation(
        self,
        table_name: str,
        summaries: list[DailyReconciliationSummary],
        mode: str = "append",
    ) -> int:
        if not summaries:
            return -1

        rows = [
            {
                "settlement_date": s.settlement_date,
                "currency": s.currency,
                "total_settled_amount": float(s.total_settled_amount),
                "transaction_count": s.transaction_count,
                "total_fees_collected": float(s.total_fees_collected),
                "avg_fx_rate": float(s.avg_fx_rate),
                "computed_at_utc": s.computed_at_utc,
            }
            for s in summaries
        ]
        table = pa.Table.from_pylist(rows, schema=GOLD_RECON_SCHEMA)
        dest = self._get_table_path(table_name)
        write_deltalake(dest, table, mode=mode, partition_by=["settlement_date"])
        return DeltaTable(dest).version()

    def read_table(
        self,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[datetime] = None,
    ) -> pa.Table:
        dest = self._get_table_path(table_name)
        dt = DeltaTable(dest, version=version)
        return dt.to_pyarrow_table()

    def get_table_history(self, table_name: str) -> list[dict[str, Any]]:
        dest = self._get_table_path(table_name)
        dt = DeltaTable(dest)
        return dt.history()

    def table_exists(self, table_name: str) -> bool:
        dest = self._get_table_path(table_name)
        log_dir = Path(dest) / "_delta_log"
        return log_dir.exists()
