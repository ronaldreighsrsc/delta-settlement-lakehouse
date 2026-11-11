"""Software-Defined Assets wiring Delta Lake ingestion/contracts into dbt-duckdb SQL marts."""

from datetime import datetime, timezone
import json
from pathlib import Path

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

from delta_settlement_lakehouse.engines.dbt_bridge import (
    DEFAULT_DBT_PROJECT_DIR,
    DEFAULT_DUCKDB_PATH,
    DEFAULT_LAKEHOUSE_BASE_PATH,
    DeltaAnalyticsBridge,
)
from delta_settlement_lakehouse.orchestration.tasks import (
    execute_bronze_ingestion,
    execute_dbt_analytics_build,
    execute_silver_contract_gate,
)


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
        res = execute_bronze_ingestion(
            lakehouse_base_path=runtime.lakehouse_base_path,
            batch_date=runtime.batch_date,
            count=runtime.count,
            seed=runtime.seed,
            inject_chaos=runtime.inject_chaos,
            chaos_ratio=runtime.chaos_ratio,
        )
        context.log.info(
            f"Bronze ingested {res.row_count} events for {res.batch_date} (Delta v{res.delta_version})"
        )

        return Output(
            value=res.batch_date,
            metadata={
                "batch_date": res.batch_date,
                "row_count": res.row_count,
                "delta_version": res.delta_version,
                "duration_ms": res.elapsed_ms,
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
        res = execute_silver_contract_gate(
            lakehouse_base_path=runtime.lakehouse_base_path,
            duckdb_path=Path(runtime.duckdb_path),
            dbt_project_dir=Path(runtime.dbt_project_dir),
            batch_date=batch_date,
        )
        context.log.info(
            f"Silver gate [{res.batch_date}]: {res.silver_count} valid, {res.dlq_count} DLQ ({res.elapsed_ms} ms)"
        )

        yield Output(
            value=res.silver_count,
            output_name="silver_settlements",
            metadata={
                "batch_date": res.batch_date,
                "row_count": res.silver_count,
                "duration_ms": res.elapsed_ms,
            },
        )
        yield Output(
            value=res.dlq_count,
            output_name="silver_quarantine_dlq",
            metadata={
                "batch_date": res.batch_date,
                "dlq_count": res.dlq_count,
                "quarantined_at_utc": datetime.now(timezone.utc).isoformat(),
            },
        )

    @dbt_assets(manifest=manifest_path)
    def settlement_dbt_assets(context: AssetExecutionContext, runtime: LakehouseRuntimeResource):
        dbt_dir = Path(runtime.dbt_project_dir)
        res = execute_dbt_analytics_build(
            duckdb_path=Path(runtime.duckdb_path),
            dbt_project_dir=dbt_dir,
        )
        for raw_event in res.raw_events:
            yield from DbtCoreCliEventMessage(
                raw_event=raw_event,
                event_history_metadata={},
            ).to_default_asset_events(
                manifest=manifest_dict,
                dagster_dbt_translator=translator,
                context=context,
                target_path=dbt_dir / "target",
            )
        if not res.success:
            raise RuntimeError(f"dbt build failed: {res.error_message}")

    return [bronze_raw_events, silver_medallion_gate, settlement_dbt_assets]
