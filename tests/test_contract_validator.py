"""Unit tests for the Data Contract Gate and Validation Engine."""

from datetime import datetime, timezone
import json
import pytest

from delta_settlement_lakehouse.domain.constants import (
    ERR_INVALID_BIC,
    ERR_INVALID_CURRENCY,
    ERR_MALFORMED_JSON,
    ERR_MISSING_REQUIRED_FIELD,
    ERR_NON_POSITIVE_AMOUNT,
)
from delta_settlement_lakehouse.domain.models import RawPaymentEvent
from delta_settlement_lakehouse.engines.contract_validator import DataContractValidator


@pytest.fixture
def validator():
    return DataContractValidator()


def create_event(payload_dict_or_str, event_id="evt_101", batch_id="batch_01"):
    raw_payload = payload_dict_or_str if isinstance(payload_dict_or_str, str) else json.dumps(payload_dict_or_str)
    return RawPaymentEvent(
        event_id=event_id,
        batch_id=batch_id,
        ingested_at_utc=datetime.now(timezone.utc),
        ingestion_date="2026-10-08",
        source_feed="SWIFT_FIN",
        raw_payload=raw_payload,
    )


def test_validator_accepts_valid_transaction(validator):
    payload = {
        "transaction_id": "tx_valid_01",
        "timestamp_utc": "2026-10-08T10:15:30+00:00",
        "sender_bic": "CHASUS33XXX",
        "receiver_bic": "SANESMMXXXX",
        "settlement_currency": "USD",
        "settlement_amount": "50000.00",
        "target_currency": "EUR",
        "booked_fx_rate": "0.9250",
        "target_amount": "46250.00",
        "status": "SETTLED",
    }
    event = create_event(payload)
    valid_rec, dlq_rec = validator.validate_event(event)

    assert valid_rec is not None
    assert dlq_rec is None
    assert valid_rec.transaction_id == "tx_valid_01"
    assert valid_rec.settlement_amount == 50000.00
    assert len(valid_rec.reconciliation_hash) == 64


def test_validator_rejects_malformed_json(validator):
    event = create_event('{"broken_json": true, ')
    valid_rec, dlq_rec = validator.validate_event(event)

    assert valid_rec is None
    assert dlq_rec is not None
    assert dlq_rec.rejection_reason.startswith(ERR_MALFORMED_JSON)


def test_validator_rejects_unsupported_currency(validator):
    payload = {
        "transaction_id": "tx_bad_curr",
        "timestamp_utc": "2026-10-08T10:15:30+00:00",
        "sender_bic": "CHASUS33XXX",
        "receiver_bic": "SANESMMXXXX",
        "settlement_currency": "BITCOIN",
        "settlement_amount": "1000.00",
        "target_currency": "EUR",
        "booked_fx_rate": "1.0",
        "target_amount": "1000.00",
    }
    event = create_event(payload)
    valid_rec, dlq_rec = validator.validate_event(event)

    assert valid_rec is None
    assert dlq_rec is not None
    assert dlq_rec.rejection_reason.startswith(ERR_INVALID_CURRENCY)


def test_validator_rejects_negative_or_zero_amount(validator):
    payload = {
        "transaction_id": "tx_negative",
        "timestamp_utc": "2026-10-08T10:15:30+00:00",
        "sender_bic": "CHASUS33XXX",
        "receiver_bic": "SANESMMXXXX",
        "settlement_currency": "USD",
        "settlement_amount": "-250.00",
        "target_currency": "EUR",
        "booked_fx_rate": "0.9250",
        "target_amount": "-231.25",
    }
    event = create_event(payload)
    valid_rec, dlq_rec = validator.validate_event(event)

    assert valid_rec is None
    assert dlq_rec is not None
    assert dlq_rec.rejection_reason.startswith(ERR_NON_POSITIVE_AMOUNT)


def test_validator_batch_partitioning(validator):
    events = [
        create_event({
            "transaction_id": f"tx_{i}",
            "timestamp_utc": "2026-10-08T10:15:30+00:00",
            "sender_bic": "CHASUS33XXX",
            "receiver_bic": "SANESMMXXXX",
            "settlement_currency": "USD" if i % 2 == 0 else "BAD_CURR",
            "settlement_amount": "1000.00",
            "target_currency": "EUR",
            "booked_fx_rate": "0.9250",
            "target_amount": "925.00",
        }, event_id=f"evt_{i}")
        for i in range(10)
    ]

    valid_list, dlq_list = validator.validate_batch(events)
    assert len(valid_list) == 5
    assert len(dlq_list) == 5
