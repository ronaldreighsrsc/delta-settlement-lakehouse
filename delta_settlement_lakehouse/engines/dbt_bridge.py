"""Bridge connecting Delta Lake Silver/DLQ tables with DuckDB and dbt-core execution."""

from contextlib import contextmanager
from dataclasses import dataclass, field
import os
from pathlib import Path
import shutil
import sys
import time
from typing import Any, Iterator

from dbt.adapters.duckdb.connections import DuckDBConnectionManager
from dbt.cli.main import dbtRunner
from dbt_common.events.base_types import EventMsg
from dbt_common.events.functions import msg_to_dict
import duckdb
import pyarrow as pa

from delta_settlement_lakehouse.engines.delta_storage import (
    DLQ_SCHEMA,
    DeltaLakeStorage,
    SILVER_SCHEMA,
)

REPO_ROOT = Path(__file__).resolve().parents[2]
DEFAULT_DBT_PROJECT_DIR = REPO_ROOT / "dbt_analytics"
DEFAULT_LAKEHOUSE_BASE_PATH = str(REPO_ROOT / "data" / "lakehouse")
DEFAULT_DUCKDB_PATH = REPO_ROOT / "data" / "lakehouse" / "analytics.duckdb"


def resolve_dbt_executable() -> str:
    """Resolve the dbt CLI executable cleanly on both Windows virtualenvs and POSIX PATH."""
    on_path = shutil.which("dbt")
    if on_path:
        return on_path

    exe_dir = Path(sys.executable).parent
    for candidate in (
        exe_dir / "Scripts" / "dbt.exe",
        exe_dir / "dbt.exe",
        exe_dir / "bin" / "dbt",
        exe_dir / "dbt",
    ):
        if candidate.exists():
            return str(candidate)

    return "dbt"


@dataclass(frozen=True)
class DbtRunResult:
    """Structured result of a programmatic dbt invocation."""

    success: bool
    command: list[str]
    elapsed_seconds: float
    error_message: str = ""
    raw_events: list[dict[str, Any]] = field(default_factory=list)


@contextmanager
def _scoped_env(var_name: str, value: str) -> Iterator[None]:
    previous = os.environ.get(var_name)
    os.environ[var_name] = value
    try:
        yield
    finally:
        if previous is None:
            os.environ.pop(var_name, None)
        else:
            os.environ[var_name] = previous


class DeltaAnalyticsBridge:
    """Hydrates DuckDB raw_lakehouse source tables from Delta transaction logs and executes dbt."""

    SOURCE_SCHEMAS: dict[str, pa.Schema] = {
        "silver_settlements": SILVER_SCHEMA,
        "silver_quarantine_dlq": DLQ_SCHEMA,
    }

    def __init__(self, project_dir: Path = DEFAULT_DBT_PROJECT_DIR):
        self.project_dir = Path(project_dir)

    def hydrate_sources(
        self,
        lakehouse_base_path: str | Path,
        duckdb_path: Path = DEFAULT_DUCKDB_PATH,
    ) -> dict[str, int]:
        """Register committed Delta Lake Silver/DLQ snapshots inside DuckDB raw_lakehouse schema."""
        duckdb_file = Path(duckdb_path)
        duckdb_file.parent.mkdir(parents=True, exist_ok=True)
        storage = DeltaLakeStorage(base_path=lakehouse_base_path)
        row_counts: dict[str, int] = {}

        conn = duckdb.connect(str(duckdb_file))
        try:
            conn.execute("CREATE SCHEMA IF NOT EXISTS raw_lakehouse")
            for table_name, arrow_schema in self.SOURCE_SCHEMAS.items():
                if storage.table_exists(table_name):
                    arrow_table = storage.read_table(table_name)
                else:
                    arrow_table = arrow_schema.empty_table()

                row_counts[table_name] = arrow_table.num_rows
                view_name = f"_arrow_{table_name}"
                conn.register(view_name, arrow_table)
                conn.execute(
                    f"CREATE OR REPLACE TABLE raw_lakehouse.{table_name} AS SELECT * FROM {view_name}"
                )
                conn.unregister(view_name)
        finally:
            conn.close()

        return row_counts

    @staticmethod
    def _release_duckdb_handles() -> None:
        """Close cached dbt-duckdb singleton connections to prevent Windows file locks."""
        env = getattr(DuckDBConnectionManager, "_ENV", None)
        conn = getattr(env, "conn", None)
        if conn is not None and hasattr(conn, "close"):
            try:
                conn.close()
            except Exception:
                pass
        DuckDBConnectionManager.close_all_connections()

    def run_dbt(
        self,
        args: list[str],
        duckdb_path: Path = DEFAULT_DUCKDB_PATH,
    ) -> DbtRunResult:
        """Invoke dbt-core programmatically with isolated DuckDB target path."""
        start = time.perf_counter()
        duckdb_file = Path(duckdb_path)
        duckdb_file.parent.mkdir(parents=True, exist_ok=True)

        full_args = [
            *args,
            "--project-dir",
            str(self.project_dir),
            "--profiles-dir",
            str(self.project_dir),
        ]

        captured_events: list[dict[str, Any]] = []

        def _capture_event(event: EventMsg) -> None:
            captured_events.append(msg_to_dict(event))

        runner = dbtRunner(callbacks=[_capture_event])
        try:
            with _scoped_env("LAKEHOUSE_DUCKDB_PATH", str(duckdb_file.resolve())):
                res = runner.invoke(full_args)
        finally:
            self._release_duckdb_handles()

        elapsed = round(time.perf_counter() - start, 3)
        error_msg = ""
        if not res.success:
            if res.exception is not None:
                error_msg = str(res.exception)
            else:
                failures = []
                if getattr(res, "result", None) is not None:
                    for item in getattr(res.result, "results", []):
                        if str(item.status) in {"error", "fail"}:
                            failures.append(f"{item.node.name}: {item.message}")
                error_msg = "; ".join(failures) or "dbt invocation failed"

        return DbtRunResult(
            success=bool(res.success),
            command=full_args,
            elapsed_seconds=elapsed,
            error_message=error_msg,
            raw_events=captured_events,
        )

