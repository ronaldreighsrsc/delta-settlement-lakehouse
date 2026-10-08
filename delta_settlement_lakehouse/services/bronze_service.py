"""Bronze Ingestion Service: appends raw payment feeds to Delta Lake."""

from typing import Optional
from delta_settlement_lakehouse.domain.models import RawPaymentEvent
from delta_settlement_lakehouse.engines.delta_storage import DeltaLakeStorage


class BronzeService:
    """Manages raw, immutable append-only ingestion into the Bronze layer."""

    TABLE_NAME = "bronze_raw_events"

    def __init__(self, storage: DeltaLakeStorage):
        self.storage = storage

    def ingest_batch(self, events: list[RawPaymentEvent]) -> int:
        """Appends events to bronze_raw_events. Returns the written Delta table version."""
        if not events:
            return -1
        return self.storage.write_raw_events(
            table_name=self.TABLE_NAME,
            events=events,
            mode="append",
        )
