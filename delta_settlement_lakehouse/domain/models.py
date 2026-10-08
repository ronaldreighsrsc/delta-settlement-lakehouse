"""Pure domain models, enums, and data contracts for delta_settlement_lakehouse."""

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from enum import Enum
from typing import Optional


class Currency(str, Enum):
    USD = "USD"
    EUR = "EUR"
    GBP = "GBP"
    JPY = "JPY"
    CAD = "CAD"
    AUD = "AUD"
    CHF = "CHF"
    SGD = "SGD"
    CLP = "CLP"
    BRL = "BRL"
    MXN = "MXN"


class SettlementStatus(str, Enum):
    PENDING = "PENDING"
    SETTLED = "SETTLED"
    FAILED = "FAILED"
    QUARANTINED = "QUARANTINED"


@dataclass(frozen=True)
class RawPaymentEvent:
    """Represents an unparsed raw event ingested into Bronze."""

    event_id: str
    batch_id: str
    ingested_at_utc: datetime
    ingestion_date: str
    source_feed: str
    raw_payload: str


@dataclass(frozen=True)
class SettlementRecord:
    """Standardized, validated cross-border payment record stored in Silver."""

    transaction_id: str
    batch_id: str
    timestamp_utc: datetime
    sender_bic: str
    receiver_bic: str
    settlement_currency: Currency
    settlement_amount: Decimal
    target_currency: Currency
    booked_fx_rate: Decimal
    target_amount: Decimal
    status: SettlementStatus
    reconciliation_hash: str

    @property
    def settlement_year_month(self) -> str:
        return self.timestamp_utc.strftime("%Y-%m")

    @property
    def settlement_date(self) -> str:
        return self.timestamp_utc.strftime("%Y-%m-%d")


@dataclass(frozen=True)
class QuarantineRecord:
    """Corrupted or contract-violating event routed to Silver DLQ."""

    event_id: str
    batch_id: str
    quarantined_at_utc: datetime
    rejection_reason: str
    raw_payload: str


@dataclass(frozen=True)
class DailyReconciliationSummary:
    """Reconciled batch position per currency stored in Gold."""

    settlement_date: str
    currency: str
    total_settled_amount: Decimal
    transaction_count: int
    total_fees_collected: Decimal
    avg_fx_rate: Decimal
    computed_at_utc: datetime


@dataclass(frozen=True)
class CounterpartyExposureSummary:
    """Bilateral bank exposure position stored in Gold."""

    sender_bic: str
    receiver_bic: str
    currency: str
    net_volume: Decimal
    gross_volume: Decimal
    settled_tx_count: int
    as_of_date: str
