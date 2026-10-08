"""Smoke tests for the Typer CLI application."""

from typer.testing import CliRunner
import pytest

from delta_settlement_lakehouse.cli import app

runner = CliRunner()


def test_cli_help():
    result = runner.invoke(app, ["--help"])
    assert result.exit_code == 0
    assert "Modern B2B Cross-Border Settlement Lakehouse Engine" in result.stdout


def test_cli_run_pipeline_and_inspect(tmp_path):
    lakehouse_path = str(tmp_path / "cli_lakehouse")

    # Run pipeline
    run_res = runner.invoke(app, [
        "run-pipeline",
        "--base-path", lakehouse_path,
        "--count", "30",
        "--date", "2026-10-08",
        "--chaos",
        "--chaos-ratio", "0.20",
    ])
    assert run_res.exit_code == 0
    assert "Pipeline Execution Completed" in run_res.stdout

    # Inspect DLQ
    dlq_res = runner.invoke(app, [
        "inspect-dlq",
        "--base-path", lakehouse_path,
    ])
    assert dlq_res.exit_code == 0
    assert "Silver Quarantine (DLQ)" in dlq_res.stdout

    # Reconcile Gold
    recon_res = runner.invoke(app, [
        "reconcile",
        "--base-path", lakehouse_path,
        "--date", "2026-10-08",
    ])
    assert recon_res.exit_code == 0
    assert "Gold Daily Reconciliation" in recon_res.stdout
