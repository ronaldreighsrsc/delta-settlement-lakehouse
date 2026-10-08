"""End-to-end Medallion Pipeline Orchestrator."""

from dataclasses import dataclass
from delta_settlement_lakehouse.domain.models import RawPaymentEvent
from delta_settlement_lakehouse.services.bronze_service import BronzeService
from delta_settlement_lakehouse.services.gold_service import GoldService
from delta_settlement_lakehouse.services.silver_service import SilverService


@dataclass(frozen=True)
class PipelineResult:
    batch_date: str
    bronze_count: int
    silver_valid_count: int
    dlq_count: int
    gold_reconciled_currencies: int


class PipelineRunner:
    """Orchestrates end-to-end execution across Bronze, Silver, DLQ, and Gold."""

    def __init__(
        self,
        bronze_service: BronzeService,
        silver_service: SilverService,
        gold_service: GoldService,
    ):
        self.bronze = bronze_service
        self.silver = silver_service
        self.gold = gold_service

    def run_pipeline(
        self, raw_events: list[RawPaymentEvent], batch_date: str
    ) -> PipelineResult:
        # Step 1: Bronze Ingestion
        self.bronze.ingest_batch(raw_events)

        # Step 2: Silver Cleansing & DLQ Routing
        valid_records, dlq_records = self.silver.process_events(raw_events)

        # Step 3: Gold Reconciled Dimensional Marts
        gold_summaries = self.gold.reconcile_date(batch_date)

        return PipelineResult(
            batch_date=batch_date,
            bronze_count=len(raw_events),
            silver_valid_count=len(valid_records),
            dlq_count=len(dlq_records),
            gold_reconciled_currencies=len(gold_summaries),
        )
