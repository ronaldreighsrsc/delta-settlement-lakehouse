"""Data Contract Gate and Validation Engine for B2B Cross-Border Settlements."""

from datetime import datetime, timezone
from decimal import Decimal, InvalidOperation
import hashlib
import json
from typing import Optional

from delta_settlement_lakehouse.domain.constants import (
    ERR_INVALID_BIC,
    ERR_INVALID_CURRENCY,
    ERR_INVALID_TIMESTAMP,
    ERR_MALFORMED_JSON,
    ERR_MISSING_REQUIRED_FIELD,
    ERR_NON_POSITIVE_AMOUNT,
    SUPPORTED_CURRENCIES,
    SWIFT_BIC_PATTERN,
)
from delta_settlement_lakehouse.domain.models import (
    Currency,
    QuarantineRecord,
    RawPaymentEvent,
    SettlementRecord,
    SettlementStatus,
)

REQUIRED_FIELDS = [
    "transaction_id",
    "timestamp_utc",
    "sender_bic",
    "receiver_bic",
    "settlement_currency",
    "settlement_amount",
    "target_currency",
    "booked_fx_rate",
    "target_amount",
]


class DataContractValidator:
    """Evaluates incoming Bronze events against financial contracts, routing violations to DLQ."""

    def validate_event(
        self, event: RawPaymentEvent
    ) -> tuple[Optional[SettlementRecord], Optional[QuarantineRecord]]:
        # 1. JSON parsing check
        try:
            data = json.loads(event.raw_payload)
            if not isinstance(data, dict):
                return None, self._reject(event, f"{ERR_MALFORMED_JSON}: Payload is not a JSON object")
        except Exception as e:
            return None, self._reject(event, f"{ERR_MALFORMED_JSON}: {str(e)}")

        # 2. Required fields check
        for field in REQUIRED_FIELDS:
            if field not in data or data[field] is None:
                return None, self._reject(event, f"{ERR_MISSING_REQUIRED_FIELD}: Missing field '{field}'")

        # 3. Currency code check (ISO 4217)
        src_curr_str = str(data["settlement_currency"]).upper().strip()
        tgt_curr_str = str(data["target_currency"]).upper().strip()
        if src_curr_str not in SUPPORTED_CURRENCIES:
            return None, self._reject(event, f"{ERR_INVALID_CURRENCY}: '{src_curr_str}' not supported")
        if tgt_curr_str not in SUPPORTED_CURRENCIES:
            return None, self._reject(event, f"{ERR_INVALID_CURRENCY}: '{tgt_curr_str}' not supported")

        # 4. SWIFT BIC check (ISO 9362)
        sender_bic = str(data["sender_bic"]).upper().strip()
        receiver_bic = str(data["receiver_bic"]).upper().strip()
        if not SWIFT_BIC_PATTERN.match(sender_bic):
            return None, self._reject(event, f"{ERR_INVALID_BIC}: Sender BIC '{sender_bic}' invalid")
        if not SWIFT_BIC_PATTERN.match(receiver_bic):
            return None, self._reject(event, f"{ERR_INVALID_BIC}: Receiver BIC '{receiver_bic}' invalid")

        # 5. Amount & FX rate checks
        try:
            amount = Decimal(str(data["settlement_amount"]))
            fx_rate = Decimal(str(data["booked_fx_rate"]))
            target_amount = Decimal(str(data["target_amount"]))
            if amount <= Decimal("0"):
                return None, self._reject(event, f"{ERR_NON_POSITIVE_AMOUNT}: Settlement amount {amount} <= 0")
            if fx_rate <= Decimal("0"):
                return None, self._reject(event, f"{ERR_NON_POSITIVE_AMOUNT}: FX rate {fx_rate} <= 0")
        except (InvalidOperation, ValueError) as e:
            return None, self._reject(event, f"{ERR_NON_POSITIVE_AMOUNT}: Numeric conversion error: {str(e)}")

        # 6. Timestamp check
        try:
            ts_str = str(data["timestamp_utc"])
            timestamp = datetime.fromisoformat(ts_str)
            if timestamp.tzinfo is None:
                timestamp = timestamp.replace(tzinfo=timezone.utc)
        except Exception as e:
            return None, self._reject(event, f"{ERR_INVALID_TIMESTAMP}: {str(e)}")

        # 7. Compute deterministic idempotency hash
        raw_key = f"{data['transaction_id']}|{sender_bic}|{receiver_bic}|{amount}|{src_curr_str}|{timestamp.isoformat()}"
        recon_hash = hashlib.sha256(raw_key.encode("utf-8")).hexdigest()

        status_str = str(data.get("status", "SETTLED")).upper()
        status = SettlementStatus.SETTLED if status_str == "SETTLED" else SettlementStatus.PENDING

        record = SettlementRecord(
            transaction_id=str(data["transaction_id"]),
            batch_id=event.batch_id,
            timestamp_utc=timestamp,
            sender_bic=sender_bic,
            receiver_bic=receiver_bic,
            settlement_currency=Currency(src_curr_str),
            settlement_amount=amount,
            target_currency=Currency(tgt_curr_str),
            booked_fx_rate=fx_rate,
            target_amount=target_amount,
            status=status,
            reconciliation_hash=recon_hash,
        )

        return record, None

    def validate_batch(
        self, events: list[RawPaymentEvent]
    ) -> tuple[list[SettlementRecord], list[QuarantineRecord]]:
        valid_records: list[SettlementRecord] = []
        quarantine_records: list[QuarantineRecord] = []

        for event in events:
            valid, dlq = self.validate_event(event)
            if valid is not None:
                valid_records.append(valid)
            elif dlq is not None:
                quarantine_records.append(dlq)

        return valid_records, quarantine_records

    def _reject(self, event: RawPaymentEvent, reason: str) -> QuarantineRecord:
        return QuarantineRecord(
            event_id=event.event_id,
            batch_id=event.batch_id,
            quarantined_at_utc=datetime.now(timezone.utc),
            rejection_reason=reason,
            raw_payload=event.raw_payload,
        )
