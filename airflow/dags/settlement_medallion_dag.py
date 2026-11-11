"""Apache Airflow TaskFlow DAG orchestrating the Medallion Settlement Lakehouse."""

from __future__ import annotations

from datetime import datetime, timedelta
import os
from pathlib import Path
import re
from typing import Any

from airflow.decorators import dag, task
from airflow.models.param import Param
from airflow.operators.python import get_current_context

DEFAULT_PROJECT_ROOT = Path(
    os.environ.get("LAKEHOUSE_PROJECT_ROOT", str(Path(__file__).resolve().parents[2]))
)
LAKEHOUSE_PYTHON = os.environ.get("LAKEHOUSE_PYTHON_BIN", "/opt/lakehouse_venv/bin/python")
_DATE_PATTERN = re.compile(r"^\d{4}-\d{2}-\d{2}$")


@dag(
    dag_id="settlement_medallion_dag",
    description="Daily Medallion Lakehouse pipeline: Bronze Delta -> Silver/DLQ Contract Gate -> dbt-duckdb Gold Marts",
    schedule="@daily",
    start_date=datetime(2026, 10, 1),
    catchup=False,
    max_active_runs=1,
    default_args={
        "owner": "data-engineering",
        "depends_on_past": False,
        "retries": 2,
        "retry_delay": timedelta(minutes=2),
    },
    params={
        "batch_date": Param(
            "",
            type=["string", "null"],
            description="Optional YYYY-MM-DD override (defaults to {{ ds }})",
        ),
        "count": Param(100, type="integer", minimum=1, maximum=10000),
        "seed": Param(42, type="integer"),
        "chaos_ratio": Param(0.08, type="number", minimum=0.0, maximum=1.0),
    },
    tags=["medallion", "delta-lake", "duckdb", "dbt"],
)
def settlement_medallion_dag() -> None:
    """Define Bronze -> Silver/DLQ -> dbt-duckdb Gold Medallion execution DAG."""

    @task
    def prepare_batch_config() -> dict[str, Any]:
        """Resolve Airflow logical date (ds), DAG params, and Lakehouse runtime paths."""
        context = get_current_context()
        params = context.get("params") or {}
        batch_date = str(params.get("batch_date") or context["ds"])
        if not _DATE_PATTERN.match(batch_date):
            raise ValueError(f"Invalid batch_date '{batch_date}'; expected YYYY-MM-DD.")

        lakehouse_base_path = os.environ.get(
            "LAKEHOUSE_BASE_PATH",
            str(DEFAULT_PROJECT_ROOT / "data" / "lakehouse"),
        )
        duckdb_path = os.environ.get(
            "LAKEHOUSE_DUCKDB_PATH",
            str(Path(lakehouse_base_path) / "analytics.duckdb"),
        )
        dbt_project_dir = os.environ.get(
            "LAKEHOUSE_DBT_PROJECT_DIR",
            str(DEFAULT_PROJECT_ROOT / "dbt_analytics"),
        )
        return {
            "batch_date": batch_date,
            "count": int(params.get("count", 100)),
            "seed": int(params.get("seed", 42)),
            "chaos_ratio": float(params.get("chaos_ratio", 0.08)),
            "lakehouse_base_path": lakehouse_base_path,
            "duckdb_path": duckdb_path,
            "dbt_project_dir": dbt_project_dir,
        }

    @task.external_python(
        python=LAKEHOUSE_PYTHON,
        expect_airflow=False,
        expect_pendulum=False,
    )
    def bronze_ingest(batch_cfg: dict[str, Any]) -> dict[str, Any]:
        """Generate and overwrite idempotent Bronze raw batch inside isolated Lakehouse venv."""
        from delta_settlement_lakehouse.orchestration.tasks import (
            execute_bronze_ingestion,
            validate_batch_date,
        )

        batch_date = validate_batch_date(str(batch_cfg["batch_date"]))
        chaos_ratio = float(batch_cfg["chaos_ratio"])
        result = execute_bronze_ingestion(
            lakehouse_base_path=str(batch_cfg["lakehouse_base_path"]),
            batch_date=batch_date,
            count=int(batch_cfg["count"]),
            seed=int(batch_cfg["seed"]),
            inject_chaos=chaos_ratio > 0.0,
            chaos_ratio=chaos_ratio,
        )
        return {
            **batch_cfg,
            "bronze_row_count": result.row_count,
            "delta_version": result.delta_version,
            "bronze_elapsed_ms": result.elapsed_ms,
        }

    @task.external_python(
        python=LAKEHOUSE_PYTHON,
        expect_airflow=False,
        expect_pendulum=False,
    )
    def silver_contract_gate(bronze_meta: dict[str, Any]) -> dict[str, Any]:
        """Validate Pydantic settlement contracts, route to Silver/DLQ, and hydrate DuckDB."""
        from pathlib import Path

        from delta_settlement_lakehouse.orchestration.tasks import (
            execute_silver_contract_gate,
            validate_batch_date,
        )

        batch_date = validate_batch_date(str(bronze_meta["batch_date"]))
        result = execute_silver_contract_gate(
            lakehouse_base_path=str(bronze_meta["lakehouse_base_path"]),
            duckdb_path=Path(bronze_meta["duckdb_path"]),
            dbt_project_dir=Path(bronze_meta["dbt_project_dir"]),
            batch_date=batch_date,
        )
        return {
            **bronze_meta,
            "silver_count": result.silver_count,
            "dlq_count": result.dlq_count,
            "silver_elapsed_ms": result.elapsed_ms,
        }

    @task.external_python(
        python=LAKEHOUSE_PYTHON,
        expect_airflow=False,
        expect_pendulum=False,
    )
    def dbt_analytics_build(silver_meta: dict[str, Any]) -> dict[str, Any]:
        """Run dbt staging, Kimball marts, and YAML/SQL contract tests in DuckDB."""
        from pathlib import Path

        from delta_settlement_lakehouse.orchestration.tasks import (
            execute_dbt_analytics_build,
        )

        result = execute_dbt_analytics_build(
            duckdb_path=Path(silver_meta["duckdb_path"]),
            dbt_project_dir=Path(silver_meta["dbt_project_dir"]),
        )
        if not result.success:
            raise RuntimeError(f"dbt build failed: {result.error_message}")

        return {
            **silver_meta,
            "dbt_success": result.success,
            "dbt_command": " ".join(result.command),
            "dbt_elapsed_seconds": round(result.elapsed_seconds, 2),
            "dbt_events_count": len(result.raw_events),
        }

    batch_cfg = prepare_batch_config()
    bronze_meta = bronze_ingest(batch_cfg)
    silver_meta = silver_contract_gate(bronze_meta)
    dbt_analytics_build(silver_meta)


settlement_medallion_dag()
