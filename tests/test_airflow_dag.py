"""Tests for Spec 003: shared Medallion task primitives, Airflow TaskFlow DAG, and Docker stack."""

import ast
from pathlib import Path
import subprocess
import sys
import duckdb
import pytest
import yaml

from delta_settlement_lakehouse.engines.delta_storage import DeltaLakeStorage
from delta_settlement_lakehouse.orchestration.tasks import (
    execute_bronze_ingestion,
    execute_dbt_analytics_build,
    execute_silver_contract_gate,
)

PROJECT_ROOT = Path(__file__).resolve().parents[1]
AIRFLOW_DAG_FILE = PROJECT_ROOT / "airflow" / "dags" / "settlement_medallion_dag.py"


@pytest.fixture(scope="module")
def airflow_test_duckdb(tmp_path_factory: pytest.TempPathFactory) -> Path:
    return tmp_path_factory.mktemp("airflow_tasks_duckdb") / "analytics.duckdb"


@pytest.fixture
def isolated_task_env(tmp_path: Path, airflow_test_duckdb: Path):
    base_path = str(tmp_path / "lakehouse")
    airflow_test_duckdb.unlink(missing_ok=True)
    yield base_path, airflow_test_duckdb
    airflow_test_duckdb.unlink(missing_ok=True)


def test_shared_tasks_execute_medallion_pipeline_and_idempotency(isolated_task_env):
    """RF-1 & Edge Case 2: Shared Airflow/Dagster task callables execute idempotently end-to-end."""
    base_path, duckdb_path = isolated_task_env
    batch_date = "2026-10-28"

    for _ in range(2):
        bronze_res = execute_bronze_ingestion(
            lakehouse_base_path=base_path,
            batch_date=batch_date,
            count=30,
            seed=42,
            inject_chaos=True,
            chaos_ratio=0.10,
        )
        assert bronze_res.row_count == 30

        silver_res = execute_silver_contract_gate(
            lakehouse_base_path=base_path,
            duckdb_path=duckdb_path,
            batch_date=batch_date,
        )
        assert silver_res.silver_count + silver_res.dlq_count == 30

        dbt_res = execute_dbt_analytics_build(duckdb_path=duckdb_path)
        assert dbt_res.success, dbt_res.error_message

    storage = DeltaLakeStorage(base_path=base_path)
    assert storage.read_table("bronze_raw_events").num_rows == 30
    assert storage.read_table("silver_settlements").num_rows == silver_res.silver_count

    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        mart_count = conn.execute(
            "SELECT SUM(transaction_count) FROM main_marts.fct_daily_settlements"
        ).fetchone()[0]
    finally:
        conn.close()
    assert mart_count == silver_res.silver_count


def test_shared_tasks_handle_empty_silver_batch(isolated_task_env):
    """Edge Case 1: 100% DLQ batch completes dbt_analytics_build safely with 0 Silver rows."""
    base_path, duckdb_path = isolated_task_env
    execute_bronze_ingestion(
        lakehouse_base_path=base_path,
        batch_date="2026-10-29",
        count=8,
        seed=7,
        inject_chaos=True,
        chaos_ratio=1.0,
    )
    silver_res = execute_silver_contract_gate(
        lakehouse_base_path=base_path,
        duckdb_path=duckdb_path,
        batch_date="2026-10-29",
    )
    assert silver_res.silver_count == 0
    assert silver_res.dlq_count == 8

    dbt_res = execute_dbt_analytics_build(duckdb_path=duckdb_path)
    assert dbt_res.success, dbt_res.error_message


def test_airflow_taskflow_dag_ast_contract_and_dependencies():
    """RF-2, RF-3 & Edge Case 3: TaskFlow DAG structure validates safely on Windows without POSIX imports."""
    assert AIRFLOW_DAG_FILE.exists(), f"Missing Airflow DAG file: {AIRFLOW_DAG_FILE}"
    source = AIRFLOW_DAG_FILE.read_text(encoding="utf-8")
    tree = ast.parse(source, filename=str(AIRFLOW_DAG_FILE))

    # Top-level module should import TaskFlow decorators and shared tasks cleanly
    imports = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.ImportFrom) and node.module:
            for alias in node.names:
                imports.add(f"{node.module}.{alias.name}")
    assert "airflow.decorators.dag" in imports
    assert "airflow.decorators.task" in imports
    assert "delta_settlement_lakehouse.orchestration.tasks.execute_bronze_ingestion" in imports
    assert "delta_settlement_lakehouse.orchestration.tasks.execute_silver_contract_gate" in imports
    assert "delta_settlement_lakehouse.orchestration.tasks.execute_dbt_analytics_build" in imports

    # Locate @dag-decorated pipeline factory and assert schedule, catchup, and retries
    dag_factories = []
    task_names = set()
    external_python_tasks = set()
    for node in ast.walk(tree):
        if isinstance(node, ast.FunctionDef):
            for dec in node.decorator_list:
                if isinstance(dec, ast.Call) and isinstance(dec.func, ast.Name):
                    if dec.func.id == "dag":
                        dag_factories.append((node.name, dec))
                    elif dec.func.id == "task":
                        task_names.add(node.name)
                elif isinstance(dec, ast.Name) and dec.id == "task":
                    task_names.add(node.name)
                elif (
                    isinstance(dec, ast.Call)
                    and isinstance(dec.func, ast.Attribute)
                    and isinstance(dec.func.value, ast.Name)
                    and dec.func.value.id == "task"
                    and dec.func.attr == "external_python"
                ):
                    task_names.add(node.name)
                    external_python_tasks.add(node.name)

    assert len(dag_factories) == 1
    _, dag_dec = dag_factories[0]
    kw = {k.arg: ast.literal_eval(k.value) for k in dag_dec.keywords if k.arg in {"dag_id", "schedule", "catchup"}}
    assert kw["dag_id"] == "settlement_medallion_dag"
    assert kw["schedule"] == "@daily"
    assert kw["catchup"] is False
    assert "retries" in source

    # All Medallion stages, isolated venv tasks, and logical-date propagation must be present
    assert {"prepare_batch_config", "bronze_ingest", "silver_contract_gate", "dbt_analytics_build"}.issubset(task_names)
    assert {"bronze_ingest", "silver_contract_gate", "dbt_analytics_build"}.issubset(external_python_tasks)
    assert "ds" in source
    assert "settlement_medallion_dag()" in source


def test_docker_compose_and_airflow_image_contracts():
    """RF-4 & RNF-2: Validate containerized Airflow stack definition, env template, and gitignore."""
    dockerfile = PROJECT_ROOT / "Dockerfile.airflow"
    compose_file = PROJECT_ROOT / "docker-compose.yml"
    env_example = PROJECT_ROOT / ".env.example"
    gitignore = PROJECT_ROOT / ".gitignore"

    assert dockerfile.exists(), "Missing Dockerfile.airflow"
    assert compose_file.exists(), "Missing docker-compose.yml"
    assert env_example.exists(), "Missing .env.example"

    dockerfile_text = dockerfile.read_text(encoding="utf-8")
    assert "FROM apache/airflow:2.10.5-python3.12" in dockerfile_text
    assert "dbt-duckdb" in dockerfile_text
    assert "deltalake" in dockerfile_text
    assert "duckdb" in dockerfile_text

    compose_data = yaml.safe_load(compose_file.read_text(encoding="utf-8"))
    services = compose_data.get("services", {})
    assert {"postgres", "airflow-init", "airflow-webserver", "airflow-scheduler"}.issubset(services.keys())

    assert "healthcheck" in services["postgres"]
    assert "LocalExecutor" in str(compose_data)
    assert "/opt/airflow/project" in str(services["airflow-scheduler"].get("volumes", []))
    assert "/opt/airflow/project/airflow/dags" in str(compose_data)

    # Secrets must be parameterized cleanly via env defaults or .env
    env_text = env_example.read_text(encoding="utf-8")
    assert "POSTGRES_USER=" in env_text
    assert "POSTGRES_PASSWORD=" in env_text
    assert "AIRFLOW_ADMIN_USER=" in env_text

    gitignore_text = gitignore.read_text(encoding="utf-8")
    assert "airflow/logs/" in gitignore_text
    assert ".env" in gitignore_text


def test_docker_compose_cloud_profile_and_s3_dag_contracts():
    """Spec 004 T3 / RF-6: Opt-in cloud Compose profile wires s3-mock, terraform-init, and Airflow S3 env."""
    s3_dockerfile = PROJECT_ROOT / "Dockerfile.s3-mock"
    compose_file = PROJECT_ROOT / "docker-compose.yml"
    env_example = PROJECT_ROOT / ".env.example"

    assert s3_dockerfile.exists(), "Missing Dockerfile.s3-mock"
    s3_df_text = s3_dockerfile.read_text(encoding="utf-8")
    assert "moto" in s3_df_text
    assert "moto_server" in s3_df_text

    compose_data = yaml.safe_load(compose_file.read_text(encoding="utf-8"))
    services = compose_data.get("services", {})
    assert {"s3-mock", "terraform-init", "airflow-cloud"}.issubset(services.keys())

    for svc_name in ("s3-mock", "terraform-init", "airflow-cloud"):
        assert "cloud" in services[svc_name].get("profiles", []), f"{svc_name} must be opt-in under profile 'cloud'"

    # Default local Airflow services must remain active with no profile lock
    for local_svc in ("postgres", "airflow-init", "airflow-webserver", "airflow-scheduler"):
        assert not services[local_svc].get("profiles"), f"{local_svc} must remain active in default profile"

    assert "hashicorp/terraform:1.9" in str(services["terraform-init"].get("image", ""))
    tf_cmd = " ".join(str(x) for x in services["terraform-init"].get("command", []))
    assert "apply" in tf_cmd and "local-docker.tfvars" in tf_cmd

    cloud_env = services["airflow-cloud"].get("environment", {})
    assert cloud_env.get("LAKEHOUSE_S3_BRONZE_BUCKET") == "s3://settlement-lakehouse-bronze-dev"
    assert cloud_env.get("AWS_ENDPOINT_URL") == "http://s3-mock:5000"
    assert cloud_env.get("AWS_ALLOW_HTTP") == "true"
    assert cloud_env.get("AWS_S3_ALLOW_UNSAFE_RENAME") == "true"

    dag_text = AIRFLOW_DAG_FILE.read_text(encoding="utf-8")
    assert 'startswith("s3://")' in dag_text, "prepare_batch_config must keep DuckDB local when base path is s3://"

    env_text = env_example.read_text(encoding="utf-8")
    assert "LAKEHOUSE_S3_BRONZE_BUCKET" in env_text
    assert "AWS_ENDPOINT_URL" in env_text


def test_orchestration_tasks_import_without_dagster_dependency():
    """Isolated runtimes (Airflow /opt/lakehouse_venv) can import tasks.py without Dagster installed."""
    probe_code = (
        "import sys; sys.modules['dagster'] = None; sys.modules['dagster_dbt'] = None; "
        "from delta_settlement_lakehouse.orchestration.tasks import execute_bronze_ingestion; "
        "print(callable(execute_bronze_ingestion))"
    )
    proc = subprocess.run(
        [sys.executable, "-c", probe_code],
        cwd=str(PROJECT_ROOT),
        capture_output=True,
        text=True,
        check=False,
    )
    assert proc.returncode == 0, proc.stderr
    assert "True" in proc.stdout


def test_shared_tasks_multi_date_same_default_seed_preserves_uniqueness(isolated_task_env):
    """Daily scheduled runs using the same default seed=42 across dates must never collide on transaction_id."""
    base_path, duckdb_path = isolated_task_env

    for batch_date in ("2026-10-08", "2026-10-28"):
        execute_bronze_ingestion(
            lakehouse_base_path=base_path,
            batch_date=batch_date,
            count=25,
            seed=42,
            inject_chaos=True,
            chaos_ratio=0.12,
        )
        execute_silver_contract_gate(
            lakehouse_base_path=base_path,
            duckdb_path=duckdb_path,
            batch_date=batch_date,
        )

    dbt_res = execute_dbt_analytics_build(duckdb_path=duckdb_path)
    assert dbt_res.success, dbt_res.error_message

    conn = duckdb.connect(str(duckdb_path), read_only=True)
    try:
        distinct_dates = conn.execute(
            "SELECT COUNT(DISTINCT settlement_date) FROM main_marts.fct_daily_settlements"
        ).fetchone()[0]
    finally:
        conn.close()
    assert distinct_dates == 2
