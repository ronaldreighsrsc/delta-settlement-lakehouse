"""Silver Processing Service: cleanses bronze events, routes to silver and DLQ."""

from datetime import datetime, timezone
import pyarrow.compute as pc

from delta_settlement_lakehouse.domain.models import (
    QuarantineRecord,
    RawPaymentEvent,
    SettlementRecord,
)
from delta_settlement_lakehouse.engines.contract_validator import DataContractValidator
from delta_settlement_lakehouse.engines.delta_storage import DeltaLakeStorage


class SilverService:
    """Evaluates data contracts, routes cleansed records to Silver, and diverts failures to DLQ."""

    SILVER_TABLE = "silver_settlements"
    DLQ_TABLE = "silver_quarantine_dlq"

    def __init__(self, storage: DeltaLakeStorage, validator: DataContractValidator):
        self.storage = storage
        self.validator = validator

    def process_events(
        self, events: list[RawPaymentEvent]
    ) -> tuple[list[SettlementRecord], list[QuarantineRecord]]:
        valid_records, quarantine_records = self.validator.validate_batch(events)

        if valid_records:
            self.storage.write_settlements(
                table_name=self.SILVER_TABLE,
                records=valid_records,
                mode="append",
            )

        if quarantine_records:
            self.storage.write_quarantine(
                table_name=self.DLQ_TABLE,
                records=quarantine_records,
                mode="append",
            )

        return valid_records, quarantine_records

    def process_bronze_date(self, ingestion_date: str) -> tuple[int, int]:
        """Reads bronze table for given date, applies contract gate, and writes to silver and DLQ."""
        if not self.storage.table_exists("bronze_raw_events"):
            return 0, 0

        bronze_table = self.storage.read_table("bronze_raw_events")
        mask = pc.equal(bronze_table["ingestion_date"], ingestion_date)
        filtered = bronze_table.filter(mask)

        if filtered.num_rows == 0:
            return 0, 0

        pylist = filtered.to_pylist()
        events = [
            RawPaymentEvent(
                event_id=row["event_id"],
                batch_id=row["batch_id"],
                ingested_at_utc=row["ingested_at_utc"],
                ingestion_date=row["ingestion_date"],
                source_feed=row["source_feed"],
                raw_payload=row["raw_payload"],
            )
            for row in pylist
        ]

        valid, dlq = self.process_events(events)
        return len(valid), len(dlq)
