"""Delta Lake Storage and Time Travel Engine via delta-rs and PyArrow."""

from datetime import datetime
import os
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

MEDALLION_TABLE_ZONES: dict[str, str] = {
    "bronze_raw_events": "bronze",
    "silver_settlements": "silver",
    "silver_quarantine_dlq": "dlq",
    "gold_daily_reconciliation": "gold",
}

ZONE_ENV_VARS: dict[str, str] = {
    "bronze": "LAKEHOUSE_S3_BRONZE_BUCKET",
    "silver": "LAKEHOUSE_S3_SILVER_BUCKET",
    "dlq": "LAKEHOUSE_S3_DLQ_BUCKET",
    "gold": "LAKEHOUSE_S3_GOLD_BUCKET",
}

_DELTA_AWS_ENV_KEYS: tuple[str, ...] = (
    "AWS_ENDPOINT_URL",
    "AWS_ACCESS_KEY_ID",
    "AWS_SECRET_ACCESS_KEY",
    "AWS_SESSION_TOKEN",
    "AWS_REGION",
    "AWS_ALLOW_HTTP",
    "AWS_S3_ALLOW_UNSAFE_RENAME",
)


def is_s3_uri(path_or_uri: Union[str, Path]) -> bool:
    """Return True when a path string represents an S3 object-storage URI."""
    return str(path_or_uri).strip().lower().startswith("s3://")


def normalize_s3_prefix(bucket_or_uri: Union[str, Path]) -> str:
    """Normalize a bare bucket name or s3:// URI into a clean s3:// prefix without trailing slash."""
    raw = str(bucket_or_uri).strip().rstrip("/")
    if not raw:
        raise ValueError("S3 bucket or URI cannot be empty.")
    if is_s3_uri(raw):
        return f"s3://{raw[5:].lstrip('/')}"
    return f"s3://{raw.lstrip('/')}"


def collect_s3_storage_options_from_env() -> dict[str, str]:
    """Collect non-empty AWS and delta-rs object store options from process environment."""
    opts: dict[str, str] = {}
    for key in _DELTA_AWS_ENV_KEYS:
        val = os.environ.get(key, "").strip()
        if val:
            opts[key] = val
    if "AWS_REGION" not in opts:
        default_region = os.environ.get("AWS_DEFAULT_REGION", "").strip()
        if default_region:
            opts["AWS_REGION"] = default_region
    return opts


def collect_zone_buckets_from_env() -> dict[str, str]:
    """Collect configured per-zone Medallion S3 buckets from environment variables."""
    zones: dict[str, str] = {}
    for zone, env_key in ZONE_ENV_VARS.items():
        val = os.environ.get(env_key, "").strip()
        if val:
            zones[zone] = normalize_s3_prefix(val)
    return zones

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
    """Encapsulates ACID reads, atomic writes, partitioning, and time travel over local or S3 Delta tables."""

    def __init__(
        self,
        base_path: Union[str, Path] = "data/lakehouse",
        storage_options: Optional[dict[str, str]] = None,
        zone_buckets: Optional[dict[str, str]] = None,
    ):
        self._base_is_s3 = is_s3_uri(base_path)
        self.base_path: Union[str, Path] = (
            normalize_s3_prefix(base_path) if self._base_is_s3 else Path(base_path)
        )
        self.zone_buckets: dict[str, str] = {
            zone.strip().lower(): normalize_s3_prefix(bucket)
            for zone, bucket in (zone_buckets or {}).items()
            if str(bucket).strip()
        }
        self.is_s3: bool = self._base_is_s3 or bool(self.zone_buckets)

        if storage_options is not None:
            self.storage_options: dict[str, str] = dict(storage_options)
        elif self.is_s3:
            self.storage_options = collect_s3_storage_options_from_env()
        else:
            self.storage_options = {}

        if not self.is_s3:
            Path(self.base_path).mkdir(parents=True, exist_ok=True)

    @classmethod
    def from_env(
        cls,
        base_path: Optional[Union[str, Path]] = None,
        storage_options: Optional[dict[str, str]] = None,
        zone_buckets: Optional[dict[str, str]] = None,
    ) -> "DeltaLakeStorage":
        """Construct storage resolving LAKEHOUSE_S3_* zone buckets, LAKEHOUSE_BASE_PATH, and AWS options."""
        resolved_zones = dict(collect_zone_buckets_from_env())
        if zone_buckets:
            for zone, bucket in zone_buckets.items():
                if str(bucket).strip():
                    resolved_zones[zone.strip().lower()] = normalize_s3_prefix(bucket)

        env_base = os.environ.get("LAKEHOUSE_BASE_PATH", "").strip()
        effective_base = env_base or (base_path if base_path is not None else "data/lakehouse")

        effective_opts = collect_s3_storage_options_from_env()
        if storage_options:
            effective_opts.update(storage_options)

        has_s3 = is_s3_uri(effective_base) or bool(resolved_zones)
        return cls(
            base_path=effective_base,
            storage_options=effective_opts if (has_s3 or storage_options is not None) else None,
            zone_buckets=resolved_zones or None,
        )

    def get_table_uri(self, table_name: str) -> str:
        """Resolve local filesystem path or s3:// URI for a logical Medallion Delta table."""
        zone = MEDALLION_TABLE_ZONES.get(table_name)
        if zone and zone in self.zone_buckets:
            return f"{self.zone_buckets[zone]}/{table_name}"
        if self._base_is_s3:
            return f"{self.base_path}/{table_name}"
        if self.zone_buckets:
            fallback_bucket = next(iter(self.zone_buckets.values()))
            return f"{fallback_bucket}/{table_name}"
        return str((Path(self.base_path) / table_name).resolve())

    def _get_table_path(self, table_name: str) -> str:
        return self.get_table_uri(table_name)

    def _effective_storage_options(self, dest_uri: str) -> Optional[dict[str, str]]:
        if is_s3_uri(dest_uri):
            return dict(self.storage_options) if self.storage_options else None
        return None

    def _write_delta(
        self,
        table_name: str,
        table: pa.Table,
        *,
        mode: str = "append",
        partition_by: Optional[list[str]] = None,
    ) -> int:
        dest = self.get_table_uri(table_name)
        opts = self._effective_storage_options(dest)
        write_deltalake(dest, table, mode=mode, partition_by=partition_by, storage_options=opts)
        return DeltaTable(dest, storage_options=opts).version()

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
        return self._write_delta(
            table_name,
            table,
            mode=mode,
            partition_by=["ingestion_date"],
        )

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
        return self._write_delta(
            table_name,
            table,
            mode=mode,
            partition_by=["settlement_currency", "settlement_year_month"],
        )

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
        return self._write_delta(table_name, table, mode=mode)

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
        return self._write_delta(
            table_name,
            table,
            mode=mode,
            partition_by=["settlement_date"],
        )

    def read_table(
        self,
        table_name: str,
        version: Optional[int] = None,
        timestamp: Optional[datetime] = None,
    ) -> pa.Table:
        dest = self.get_table_uri(table_name)
        dt = DeltaTable(dest, version=version, storage_options=self._effective_storage_options(dest))
        return dt.to_pyarrow_table()

    def get_table_history(self, table_name: str) -> list[dict[str, Any]]:
        dest = self.get_table_uri(table_name)
        dt = DeltaTable(dest, storage_options=self._effective_storage_options(dest))
        return dt.history()

    def table_exists(self, table_name: str) -> bool:
        dest = self.get_table_uri(table_name)
        opts = self._effective_storage_options(dest)
        return DeltaTable.is_deltatable(dest, storage_options=opts)

    def delete_where(self, table_name: str, predicate: str) -> bool:
        """Delete matching rows atomically from an existing Delta table (local or s3://)."""
        if not self.table_exists(table_name):
            return False
        dest = self.get_table_uri(table_name)
        dt = DeltaTable(dest, storage_options=self._effective_storage_options(dest))
        dt.delete(predicate=predicate)
        return True


def resolve_lakehouse_storage(
    storage_cls: type[DeltaLakeStorage],
    base_path: Union[str, Path],
    *,
    default_base_path: Union[str, Path] = "data/lakehouse",
    storage_options: Optional[dict[str, str]] = None,
    zone_buckets: Optional[dict[str, str]] = None,
) -> DeltaLakeStorage:
    """Resolve DeltaLakeStorage from env when default path is active, keeping custom local paths isolated."""
    is_default = str(base_path) in {str(default_base_path), "data/lakehouse"}
    if is_default and zone_buckets is None and not is_s3_uri(base_path):
        return storage_cls.from_env(base_path=base_path, storage_options=storage_options)
    return storage_cls(
        base_path=base_path,
        storage_options=storage_options,
        zone_buckets=zone_buckets,
    )
