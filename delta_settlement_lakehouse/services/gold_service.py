"""Gold Reconciliation Service: aggregates cleansed silver transactions into analytical marts."""

from typing import Optional
from delta_settlement_lakehouse.domain.models import CounterpartyExposureSummary, DailyReconciliationSummary
from delta_settlement_lakehouse.engines.delta_storage import DeltaLakeStorage
from delta_settlement_lakehouse.engines.olap_engine import OlapEngine


class GoldService:
    """Computes and stores analytical reconciliation summaries and counterparty exposures in Gold."""

    RECON_TABLE = "gold_daily_reconciliation"

    def __init__(self, storage: DeltaLakeStorage, olap: OlapEngine):
        self.storage = storage
        self.olap = olap

    def reconcile_date(self, settlement_date: str) -> list[DailyReconciliationSummary]:
        if not self.storage.table_exists("silver_settlements"):
            return []

        silver_table = self.storage.read_table("silver_settlements")
        summaries = self.olap.compute_daily_reconciliation(silver_table, settlement_date)

        if summaries:
            self.storage.write_reconciliation(
                table_name=self.RECON_TABLE,
                summaries=summaries,
                mode="append",
            )

        return summaries

    def compute_exposures(self, settlement_date: str) -> list[CounterpartyExposureSummary]:
        if not self.storage.table_exists("silver_settlements"):
            return []

        silver_table = self.storage.read_table("silver_settlements")
        return self.olap.compute_counterparty_exposure(silver_table, settlement_date)
