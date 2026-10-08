"""Unit tests for domain models, enums, and constants."""

from datetime import datetime, timezone
from decimal import Decimal
import pytest

from delta_settlement_lakehouse.domain.constants import SUPPORTED_CURRENCIES, SWIFT_BIC_REGEX
from delta_settlement_lakehouse.domain.models import (
    Currency,
    DailyReconciliationSummary,
    QuarantineRecord,
    RawPaymentEvent,
    SettlementRecord,
    SettlementStatus,
)


def test_currency_enum_and_constants():
    assert Currency.USD.value == "USD"
    assert Currency.EUR.value == "EUR"
    assert Currency.CLP.value == "CLP"
    assert "USD" in SUPPORTED_CURRENCIES
    assert "XYZ" not in SUPPORTED_CURRENCIES


def test_raw_payment_event_instantiation():
    now = datetime.now(timezone.utc)
    event = RawPaymentEvent(
        event_id="evt_001",
        batch_id="batch_20261008",
        ingested_at_utc=now,
        ingestion_date="2026-10-08",
        source_feed="SWIFT_FIN",
        raw_payload='{"id": "tx_123", "amount": 10500.50}',
    )
    assert event.event_id == "evt_001"
    assert event.source_feed == "SWIFT_FIN"


def test_settlement_record_properties():
    now = datetime(2026, 10, 8, 14, 30, tzinfo=timezone.utc)
    record = SettlementRecord(
        transaction_id="tx_1001",
        batch_id="batch_01",
        timestamp_utc=now,
        sender_bic="CHASUS33XXX",
        receiver_bic="SANESMMXXXX",
        settlement_currency=Currency.USD,
        settlement_amount=Decimal("150000.00"),
        target_currency=Currency.EUR,
        booked_fx_rate=Decimal("0.9250"),
        target_amount=Decimal("138750.00"),
        status=SettlementStatus.SETTLED,
        reconciliation_hash="a1b2c3d4",
    )
    assert record.settlement_year_month == "2026-10"
    assert record.settlement_amount == Decimal("150000.00")
    assert record.status == SettlementStatus.SETTLED


def test_quarantine_record_instantiation():
    now = datetime.now(timezone.utc)
    q = QuarantineRecord(
        event_id="evt_err_01",
        batch_id="batch_01",
        quarantined_at_utc=now,
        rejection_reason="INVALID_CURRENCY: XYZ",
        raw_payload='{"currency": "XYZ"}',
    )
    assert q.event_id == "evt_err_01"
    assert "INVALID_CURRENCY" in q.rejection_reason
