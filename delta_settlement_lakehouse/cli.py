"""Modern B2B Cross-Border Settlement Lakehouse Engine CLI."""

from datetime import datetime, timezone
from typing import Optional
from rich.console import Console
from rich.panel import Panel
from rich.table import Table
import typer

from delta_settlement_lakehouse.engines.chaos_generator import SyntheticFeedGenerator
from delta_settlement_lakehouse.engines.contract_validator import DataContractValidator
from delta_settlement_lakehouse.engines.delta_storage import DeltaLakeStorage
from delta_settlement_lakehouse.engines.olap_engine import OlapEngine
from delta_settlement_lakehouse.services.bronze_service import BronzeService
from delta_settlement_lakehouse.services.gold_service import GoldService
from delta_settlement_lakehouse.services.pipeline_runner import PipelineRunner
from delta_settlement_lakehouse.services.silver_service import SilverService

app = typer.Typer(
    name="lakehouse",
    help="Modern B2B Cross-Border Settlement Lakehouse Engine CLI",
    add_completion=False,
)
console = Console()


@app.command()
def run_pipeline(
    base_path: str = typer.Option("data/lakehouse", "--base-path", "-p", help="Storage directory for Delta tables"),
    date: str = typer.Option("2026-10-08", "--date", "-d", help="Settlement date (YYYY-MM-DD)"),
    count: int = typer.Option(100, "--count", "-n", help="Number of payment transactions to generate"),
    chaos: bool = typer.Option(False, "--chaos", help="Inject chaos anomalies (corrupted data, retries)"),
    chaos_ratio: float = typer.Option(0.05, "--chaos-ratio", help="Ratio of chaotic anomalies (0.0 to 1.0)"),
    seed: int = typer.Option(42, "--seed", "-s", help="Deterministic RNG seed"),
):
    """Generates synthetic feed and executes the Medallion pipeline (Bronze -> Silver/DLQ -> Gold)."""
    console.print(Panel(
        f"[bold cyan]Delta Settlement Lakehouse Pipeline[/bold cyan]\n"
        f"Date: [green]{date}[/green] | Count: [yellow]{count}[/yellow] | Chaos: [magenta]{chaos}[/magenta] (ratio: {chaos_ratio})",
        border_style="cyan"
    ))

    storage = DeltaLakeStorage(base_path=base_path)
    validator = DataContractValidator()
    olap = OlapEngine()

    bronze_svc = BronzeService(storage=storage)
    silver_svc = SilverService(storage=storage, validator=validator)
    gold_svc = GoldService(storage=storage, olap=olap)

    runner = PipelineRunner(
        bronze_service=bronze_svc,
        silver_service=silver_svc,
        gold_service=gold_svc,
    )

    feed_gen = SyntheticFeedGenerator(seed=seed)
    with console.status("[bold green]Synthesizing clearing feed & executing Lakehouse layers...[/bold green]"):
        events = feed_gen.generate_batch(
            count=count,
            batch_date=date,
            inject_chaos=chaos,
            chaos_ratio=chaos_ratio,
        )
        result = runner.run_pipeline(raw_events=events, batch_date=date)

    summary_table = Table(title="Pipeline Execution Completed", border_style="green")
    summary_table.add_column("Layer / Metric", style="cyan", no_wrap=True)
    summary_table.add_column("Status / Count", style="bold white")
    summary_table.add_column("Details", style="dim")

    summary_table.add_row("Bronze Layer", str(result.bronze_count), "Raw append-only JSON events")
    summary_table.add_row("Silver Cleansed", str(result.silver_valid_count), "Validated & conformed settlement records")
    summary_table.add_row("Silver DLQ", f"[red]{result.dlq_count}[/red]", "Corrupted / contract-violating records quarantined")
    summary_table.add_row("Gold Marts", str(result.gold_reconciled_currencies), "Reconciled currency positions")

    console.print(summary_table)


@app.command()
def inspect_dlq(
    base_path: str = typer.Option("data/lakehouse", "--base-path", "-p", help="Storage directory"),
    limit: int = typer.Option(20, "--limit", "-l", help="Max records to display"),
):
    """Inspects quarantined events diverted by the Data Contract Gate."""
    storage = DeltaLakeStorage(base_path=base_path)
    if not storage.table_exists("silver_quarantine_dlq"):
        console.print("[yellow]No quarantine table found. All data is valid or pipeline has not run.[/yellow]")
        return

    table = storage.read_table("silver_quarantine_dlq")
    console.print(Panel(f"[bold red]Silver Quarantine (DLQ)[/bold red] - Total Corrupted Events: {table.num_rows}", border_style="red"))

    dt_rows = table.to_pylist()[:limit]
    res_table = Table(border_style="red")
    res_table.add_column("Event ID", style="cyan", no_wrap=True)
    res_table.add_column("Batch ID", style="dim")
    res_table.add_column("Rejection Reason", style="bold red")

    for r in dt_rows:
        res_table.add_row(r["event_id"], r["batch_id"], r["rejection_reason"])

    console.print(res_table)


@app.command()
def reconcile(
    base_path: str = typer.Option("data/lakehouse", "--base-path", "-p", help="Storage directory"),
    date: str = typer.Option("2026-10-08", "--date", "-d", help="Settlement date to inspect"),
):
    """Displays Gold layer daily reconciliation summary."""
    storage = DeltaLakeStorage(base_path=base_path)
    if not storage.table_exists("gold_daily_reconciliation"):
        console.print("[yellow]No gold reconciliation table found. Run pipeline first.[/yellow]")
        return

    table = storage.read_table("gold_daily_reconciliation")
    dt_rows = [r for r in table.to_pylist() if r["settlement_date"] == date]

    if not dt_rows:
        console.print(f"[yellow]No reconciliation records found for date {date}.[/yellow]")
        return

    res_table = Table(title=f"Gold Daily Reconciliation ({date})", border_style="bright_blue")
    res_table.add_column("Currency", style="bold yellow")
    res_table.add_column("Total Settled Amount", justify="right", style="green")
    res_table.add_column("Tx Count", justify="right", style="white")
    res_table.add_column("Fees Collected (15 bps)", justify="right", style="cyan")
    res_table.add_column("Avg FX Rate", justify="right", style="magenta")

    for r in dt_rows:
        res_table.add_row(
            r["currency"],
            f"{r['total_settled_amount']:,.2f}",
            str(r["transaction_count"]),
            f"{r['total_fees_collected']:,.2f}",
            f"{r['avg_fx_rate']:.4f}",
        )

    console.print(res_table)


@app.command()
def audit_table(
    table: str = typer.Option(..., "--table", "-t", help="Delta table name to audit"),
    base_path: str = typer.Option("data/lakehouse", "--base-path", "-p", help="Storage directory"),
    version: Optional[int] = typer.Option(None, "--version", "-v", help="Historical version for Time Travel"),
):
    """Performs Time Travel inspection and audits Delta transaction history."""
    storage = DeltaLakeStorage(base_path=base_path)
    if not storage.table_exists(table):
        console.print(f"[red]Table '{table}' does not exist.[/red]")
        return

    history = storage.get_table_history(table)
    console.print(Panel(f"[bold cyan]Delta Transaction History for '{table}'[/bold cyan]", border_style="cyan"))

    hist_table = Table(border_style="cyan")
    hist_table.add_column("Version", style="bold yellow", justify="right")
    hist_table.add_column("Timestamp", style="white")
    hist_table.add_column("Operation", style="green")

    for h in history:
        hist_table.add_row(
            str(h.get("version", "")),
            str(h.get("timestamp", "")),
            str(h.get("operation", "WRITE")),
        )

    console.print(hist_table)

    # Time travel snapshot
    target_table = storage.read_table(table, version=version)
    target_ver = version if version is not None else history[0].get("version", "latest")
    console.print(f"[bold green]Snapshot at version {target_ver}:[/bold green] [white]{target_table.num_rows} total rows.[/white]")


if __name__ == "__main__":
    app()
