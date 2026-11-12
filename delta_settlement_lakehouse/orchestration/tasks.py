"""Framework-agnostic Medallion pipeline task primitives shared across Dagster and Apache Airflow."""

from dataclasses import dataclass
import hashlib
from pathlib import Path
import re
import time

import pyarrow.compute as pc

from delta_settlement_lakehouse.domain.models import RawPaymentEvent
from delta_settlement_lakehouse.engines.chaos_generator import SyntheticFeedGenerator
from delta_settlement_lakehouse.engines.contract_validator import DataContractValidator
from delta_settlement_lakehouse.engines.dbt_bridge import (
    DEFAULT_DBT_PROJECT_DIR,
    DEFAULT_DUCKDB_PATH,
    DEFAULT_LAKEHOUSE_BASE_PATH,
    DbtRunResult,
    DeltaAnalyticsBridge,
)
from delta_settlement_lakehouse.engines.delta_storage import (
    DeltaLakeStorage,
    resolve_lakehouse_storage,
)
from delta_settlement_lakehouse.services.bronze_service import BronzeService
from delta_settlement_lakehouse.services.silver_service import SilverService

_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dataclass(frozen=True)
class BronzeTaskResult:
    """Structured summary emitted by the Bronze ingestion stage."""

    batch_date: str
    row_count: int
    delta_version: int
    elapsed_ms: float


@dataclass(frozen=True)
class SilverTaskResult:
    """Structured summary emitted by the Silver data-contract gate and DuckDB hydration."""

    batch_date: str
    silver_count: int
    dlq_count: int
    elapsed_ms: float


def validate_batch_date(batch_date: str) -> str:
    """Validate YYYY-MM-DD format strictly before constructing Delta SQL delete predicates."""
    if not _DATE_PATTERN.match(batch_date):
        raise ValueError(f"Invalid batch_date '{batch_date}'; expected YYYY-MM-DD.")
    return batch_date


def _derive_batch_seed(base_seed: int, batch_date: str) -> int:
    """Salt RNG seed deterministically with batch_date to avoid cross-day tx_id collisions."""
    digest = hashlib.blake2b(f"{base_seed}:{batch_date}".encode("utf-8"), digest_size=8).digest()
    return int.from_bytes(digest, "big")


def _delete_delta_partition(storage: DeltaLakeStorage, table_name: str, predicate: str) -> None:
    storage.delete_where(table_name, predicate)


def execute_bronze_ingestion(
    lakehouse_base_path: str = DEFAULT_LAKEHOUSE_BASE_PATH,
    batch_date: str = "2026-10-08",
    count: int = 100,
    seed: int = 42,
    inject_chaos: bool = True,
    chaos_ratio: float = 0.08,
    storage_options: dict[str, str] | None = None,
    zone_buckets: dict[str, str] | None = None,
) -> BronzeTaskResult:
    """Generate and idempotently ingest a deterministic batch into Delta bronze_raw_events."""
    start = time.perf_counter()
    clean_date = validate_batch_date(batch_date)
    storage = resolve_lakehouse_storage(
        DeltaLakeStorage,
        lakehouse_base_path,
        default_base_path=DEFAULT_LAKEHOUSE_BASE_PATH,
        storage_options=storage_options,
        zone_buckets=zone_buckets,
    )
    _delete_delta_partition(storage, "bronze_raw_events", f"ingestion_date = '{clean_date}'")

    effective_seed = _derive_batch_seed(seed, clean_date)
    raw_events = SyntheticFeedGenerator(seed=effective_seed).generate_batch(
        count=count,
        batch_date=clean_date,
        inject_chaos=inject_chaos,
        chaos_ratio=chaos_ratio,
    )
    version = BronzeService(storage=storage).ingest_batch(raw_events)
    elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
    return BronzeTaskResult(
        batch_date=clean_date,
        row_count=len(raw_events),
        delta_version=version,
        elapsed_ms=elapsed_ms,
    )


def execute_silver_contract_gate(
    lakehouse_base_path: str = DEFAULT_LAKEHOUSE_BASE_PATH,
    duckdb_path: Path = DEFAULT_DUCKDB_PATH,
    dbt_project_dir: Path = DEFAULT_DBT_PROJECT_DIR,
    batch_date: str = "2026-10-08",
    storage_options: dict[str, str] | None = None,
    zone_buckets: dict[str, str] | None = None,
) -> SilverTaskResult:
    """Validate Bronze batch against financial contracts, route to Silver/DLQ, and hydrate DuckDB."""
    start = time.perf_counter()
    clean_date = validate_batch_date(batch_date)
    storage = resolve_lakehouse_storage(
        DeltaLakeStorage,
        lakehouse_base_path,
        default_base_path=DEFAULT_LAKEHOUSE_BASE_PATH,
        storage_options=storage_options,
        zone_buckets=zone_buckets,
    )

    bronze_table = storage.read_table("bronze_raw_events")
    filtered = bronze_table.filter(pc.equal(bronze_table["ingestion_date"], clean_date))
    events = [
        RawPaymentEvent(
            event_id=row["event_id"],
            batch_id=row["batch_id"],
            ingested_at_utc=row["ingested_at_utc"],
            ingestion_date=row["ingestion_date"],
            source_feed=row["source_feed"],
            raw_payload=row["raw_payload"],
        )
        for row in filtered.to_pylist()
    ]

    _delete_delta_partition(storage, "silver_settlements", f"settlement_date = '{clean_date}'")
    date_token = clean_date.replace("-", "")
    _delete_delta_partition(storage, "silver_quarantine_dlq", f"batch_id LIKE 'batch_{date_token}_%'")

    silver_service = SilverService(storage=storage, validator=DataContractValidator())
    valid_records, dlq_records = silver_service.process_events(events)

    bridge = DeltaAnalyticsBridge(project_dir=Path(dbt_project_dir))
    bridge.hydrate_sources(
        lakehouse_base_path=lakehouse_base_path,
        duckdb_path=Path(duckdb_path),
        storage_options=storage_options,
        zone_buckets=zone_buckets,
    )

    elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
    return SilverTaskResult(
        batch_date=clean_date,
        silver_count=len(valid_records),
        dlq_count=len(dlq_records),
        elapsed_ms=elapsed_ms,
    )


def execute_dbt_analytics_build(
    duckdb_path: Path = DEFAULT_DUCKDB_PATH,
    dbt_project_dir: Path = DEFAULT_DBT_PROJECT_DIR,
) -> DbtRunResult:
    """Execute full dbt build (staging views, Gold Kimball marts, YAML & singular SQL tests)."""
    bridge = DeltaAnalyticsBridge(project_dir=Path(dbt_project_dir))
    return bridge.run_dbt(args=["build"], duckdb_path=Path(duckdb_path))

