"""Software-Defined Assets wiring Delta Lake ingestion/contracts into dbt-duckdb SQL marts."""

from datetime import datetime, timezone
import json
from pathlib import Path
import re
import time

from dagster import (
    AssetExecutionContext,
    AssetIn,
    AssetKey,
    AssetOut,
    ConfigurableResource,
    Output,
    asset,
    multi_asset,
)
from dagster_dbt import DagsterDbtTranslator, dbt_assets
from dagster_dbt.core.dbt_cli_invocation import DbtCoreCliEventMessage
from deltalake import DeltaTable
import pyarrow.compute as pc

from delta_settlement_lakehouse.domain.models import RawPaymentEvent
from delta_settlement_lakehouse.engines.chaos_generator import SyntheticFeedGenerator
from delta_settlement_lakehouse.engines.contract_validator import DataContractValidator
from delta_settlement_lakehouse.engines.dbt_bridge import (
    DEFAULT_DBT_PROJECT_DIR,
    DEFAULT_DUCKDB_PATH,
    DEFAULT_LAKEHOUSE_BASE_PATH,
    DeltaAnalyticsBridge,
)
from delta_settlement_lakehouse.engines.delta_storage import DeltaLakeStorage
from delta_settlement_lakehouse.services.bronze_service import BronzeService
from delta_settlement_lakehouse.services.silver_service import SilverService

_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


class LakehouseRuntimeResource(ConfigurableResource):
    """Runtime execution parameters injected into Dagster Medallion assets."""

    lakehouse_base_path: str = DEFAULT_LAKEHOUSE_BASE_PATH
    duckdb_path: str = str(DEFAULT_DUCKDB_PATH)
    dbt_project_dir: str = str(DEFAULT_DBT_PROJECT_DIR)
    batch_date: str = "2026-10-08"
    count: int = 100
    inject_chaos: bool = True
    chaos_ratio: float = 0.08
    seed: int = 42


def ensure_dbt_manifest(project_dir: Path, duckdb_path: Path | None = None) -> Path:
    """Compile dbt target/manifest.json on demand for fresh clones without checked-in targets."""
    manifest_path = Path(project_dir) / "target" / "manifest.json"
    if not manifest_path.exists():
        bridge = DeltaAnalyticsBridge(project_dir=project_dir)
        res = bridge.run_dbt(
            args=["parse"],
            duckdb_path=duckdb_path or DEFAULT_DUCKDB_PATH,
        )
        if not res.success:
            raise RuntimeError(f"Failed to compile dbt manifest: {res.error_message}")
    return manifest_path


def _validate_batch_date(batch_date: str) -> str:
    if not _DATE_PATTERN.match(batch_date):
        raise ValueError(f"Invalid batch_date '{batch_date}'; expected YYYY-MM-DD.")
    return batch_date


def _delete_delta_partition(storage: DeltaLakeStorage, table_name: str, predicate: str) -> None:
    if storage.table_exists(table_name):
        DeltaTable(storage._get_table_path(table_name)).delete(predicate=predicate)


def build_settlement_assets(project_dir: Path = DEFAULT_DBT_PROJECT_DIR) -> list:
    """Construct the full Medallion + dbt Asset list bound to the compiled dbt manifest."""
    manifest_path = ensure_dbt_manifest(project_dir=project_dir)
    manifest_dict = json.loads(manifest_path.read_text(encoding="utf-8"))
    translator = DagsterDbtTranslator()

    @asset(
        name="bronze_raw_events",
        group_name="medallion_ingestion",
        description="Deterministic raw payment stream ingestion into Delta Lake Bronze layer.",
    )
    def bronze_raw_events(
        context: AssetExecutionContext,
        runtime: LakehouseRuntimeResource,
    ) -> Output[str]:
        start = time.perf_counter()
        batch_date = _validate_batch_date(runtime.batch_date)
        storage = DeltaLakeStorage(base_path=runtime.lakehouse_base_path)
        _delete_delta_partition(storage, "bronze_raw_events", f"ingestion_date = '{batch_date}'")

        generator = SyntheticFeedGenerator(seed=runtime.seed)
        raw_events = generator.generate_batch(
            count=runtime.count,
            batch_date=batch_date,
            inject_chaos=runtime.inject_chaos,
            chaos_ratio=runtime.chaos_ratio,
        )
        version = BronzeService(storage=storage).ingest_batch(raw_events)
        elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
        context.log.info(f"Bronze ingested {len(raw_events)} events for {batch_date} (Delta v{version})")

        return Output(
            value=batch_date,
            metadata={
                "batch_date": batch_date,
                "row_count": len(raw_events),
                "delta_version": version,
                "duration_ms": elapsed_ms,
            },
        )

    @multi_asset(
        name="silver_medallion_gate",
        group_name="medallion_governance",
        ins={"batch_date": AssetIn(key=AssetKey(["bronze_raw_events"]))},
        outs={
            "silver_settlements": AssetOut(
                key=AssetKey(["raw_lakehouse", "silver_settlements"]),
                group_name="medallion_governance",
                description="Validated ISO 4217 / SWIFT BIC settlements written to Delta Silver.",
            ),
            "silver_quarantine_dlq": AssetOut(
                key=AssetKey(["raw_lakehouse", "silver_quarantine_dlq"]),
                group_name="medallion_governance",
                description="Contract-violating events diverted to Delta DLQ.",
            ),
        },
    )
    def silver_medallion_gate(
        context: AssetExecutionContext,
        runtime: LakehouseRuntimeResource,
        batch_date: str,
    ):
        start = time.perf_counter()
        clean_date = _validate_batch_date(batch_date)
        storage = DeltaLakeStorage(base_path=runtime.lakehouse_base_path)

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

        bridge = DeltaAnalyticsBridge(project_dir=Path(runtime.dbt_project_dir))
        bridge.hydrate_sources(
            lakehouse_base_path=runtime.lakehouse_base_path,
            duckdb_path=Path(runtime.duckdb_path),
        )

        elapsed_ms = round((time.perf_counter() - start) * 1000, 2)
        context.log.info(
            f"Silver gate [{clean_date}]: {len(valid_records)} valid, {len(dlq_records)} DLQ ({elapsed_ms} ms)"
        )

        yield Output(
            value=len(valid_records),
            output_name="silver_settlements",
            metadata={
                "batch_date": clean_date,
                "row_count": len(valid_records),
                "duration_ms": elapsed_ms,
            },
        )
        yield Output(
            value=len(dlq_records),
            output_name="silver_quarantine_dlq",
            metadata={
                "batch_date": clean_date,
                "dlq_count": len(dlq_records),
                "quarantined_at_utc": datetime.now(timezone.utc).isoformat(),
            },
        )

    @dbt_assets(manifest=manifest_path)
    def settlement_dbt_assets(context: AssetExecutionContext, runtime: LakehouseRuntimeResource):
        bridge = DeltaAnalyticsBridge(project_dir=Path(runtime.dbt_project_dir))
        res = bridge.run_dbt(
            args=["build"],
            duckdb_path=Path(runtime.duckdb_path),
        )
        for raw_event in res.raw_events:
            yield from DbtCoreCliEventMessage(
                raw_event=raw_event,
                event_history_metadata={},
            ).to_default_asset_events(
                manifest=manifest_dict,
                dagster_dbt_translator=translator,
                context=context,
                target_path=bridge.project_dir / "target",
            )
        if not res.success:
            raise RuntimeError(f"dbt build failed: {res.error_message}")

    return [bronze_raw_events, silver_medallion_gate, settlement_dbt_assets]
