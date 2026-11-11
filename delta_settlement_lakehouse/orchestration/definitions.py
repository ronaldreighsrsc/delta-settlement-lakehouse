"""Dagster Definitions and programmatic execution helpers for Spec 002."""

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from dagster import Definitions, materialize

from delta_settlement_lakehouse.engines.dbt_bridge import (
    DEFAULT_DBT_PROJECT_DIR,
    DEFAULT_DUCKDB_PATH,
    DEFAULT_LAKEHOUSE_BASE_PATH,
    _scoped_env,
)
from delta_settlement_lakehouse.orchestration.assets import (
    LakehouseRuntimeResource,
    build_settlement_assets,
)


@dataclass(frozen=True)
class OrchestrationRunResult:
    """Structured summary of an end-to-end Dagster asset materialization."""

    success: bool
    batch_date: str
    bronze_count: int
    silver_count: int
    dlq_count: int
    asset_metadata: dict[str, dict[str, Any]]
    duckdb_path: Path


def _build_components(
    lakehouse_base_path: str,
    duckdb_path: Path,
    dbt_project_dir: Path,
    batch_date: str,
    count: int,
    inject_chaos: bool,
    chaos_ratio: float,
    seed: int,
) -> tuple[list, dict[str, Any]]:
    dbt_dir = Path(dbt_project_dir)
    assets = build_settlement_assets(project_dir=dbt_dir)
    runtime_resource = LakehouseRuntimeResource(
        lakehouse_base_path=str(lakehouse_base_path),
        duckdb_path=str(duckdb_path),
        dbt_project_dir=str(dbt_dir),
        batch_date=batch_date,
        count=count,
        inject_chaos=inject_chaos,
        chaos_ratio=chaos_ratio,
        seed=seed,
    )
    return assets, {"runtime": runtime_resource}


def build_definitions(
    lakehouse_base_path: str = DEFAULT_LAKEHOUSE_BASE_PATH,
    duckdb_path: Path = DEFAULT_DUCKDB_PATH,
    dbt_project_dir: Path = DEFAULT_DBT_PROJECT_DIR,
    batch_date: str = "2026-10-08",
    count: int = 100,
    inject_chaos: bool = True,
    chaos_ratio: float = 0.08,
    seed: int = 42,
) -> Definitions:
    """Build Dagster Definitions usable by both `dagster dev` UI and CLI orchestration."""
    assets, resources = _build_components(
        lakehouse_base_path=lakehouse_base_path,
        duckdb_path=Path(duckdb_path),
        dbt_project_dir=Path(dbt_project_dir),
        batch_date=batch_date,
        count=count,
        inject_chaos=inject_chaos,
        chaos_ratio=chaos_ratio,
        seed=seed,
    )
    return Definitions(assets=assets, resources=resources)


def _extract_plain_metadata(metadata_entries: dict[str, Any]) -> dict[str, Any]:
    return {
        key: getattr(val, "value", val)
        for key, val in metadata_entries.items()
    }


def run_orchestrated_pipeline(
    lakehouse_base_path: str = DEFAULT_LAKEHOUSE_BASE_PATH,
    duckdb_path: Path = DEFAULT_DUCKDB_PATH,
    dbt_project_dir: Path = DEFAULT_DBT_PROJECT_DIR,
    batch_date: str = "2026-10-08",
    count: int = 100,
    inject_chaos: bool = True,
    chaos_ratio: float = 0.08,
    seed: int = 42,
) -> OrchestrationRunResult:
    """Execute the full Bronze -> Silver/DLQ -> dbt DuckDB Gold DAG in-process."""
    duckdb_file = Path(duckdb_path).resolve()
    duckdb_file.parent.mkdir(parents=True, exist_ok=True)
    assets, resources = _build_components(
        lakehouse_base_path=lakehouse_base_path,
        duckdb_path=duckdb_file,
        dbt_project_dir=Path(dbt_project_dir),
        batch_date=batch_date,
        count=count,
        inject_chaos=inject_chaos,
        chaos_ratio=chaos_ratio,
        seed=seed,
    )

    with _scoped_env("LAKEHOUSE_DUCKDB_PATH", str(duckdb_file)):
        result = materialize(assets=assets, resources=resources)

    asset_meta: dict[str, dict[str, Any]] = {}
    for event in result.get_asset_materialization_events():
        mat = event.step_materialization_data.materialization
        asset_name = mat.asset_key.path[-1]
        asset_meta[asset_name] = _extract_plain_metadata(mat.metadata)

    bronze_count = int(asset_meta.get("bronze_raw_events", {}).get("row_count", 0))
    silver_count = int(asset_meta.get("silver_settlements", {}).get("row_count", 0))
    dlq_count = int(asset_meta.get("silver_quarantine_dlq", {}).get("dlq_count", 0))

    return OrchestrationRunResult(
        success=bool(result.success),
        batch_date=batch_date,
        bronze_count=bronze_count,
        silver_count=silver_count,
        dlq_count=dlq_count,
        asset_metadata=asset_meta,
        duckdb_path=duckdb_file,
    )


defs = build_definitions()
