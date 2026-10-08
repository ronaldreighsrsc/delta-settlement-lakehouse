"""Synthetic B2B Cross-Border Payment Feed and Chaos Engineering Generator."""

import json
import random
import uuid
from datetime import datetime, timezone
from decimal import Decimal
from typing import Optional

from delta_settlement_lakehouse.domain.models import RawPaymentEvent

MAJOR_BICS = [
    "CHASUS33XXX",  # JPMorgan Chase NY
    "CITIUS33XXX",  # Citibank NA
    "BARCGB22XXX",  # Barclays London
    "BNPAFRPPXXX",  # BNP Paribas Paris
    "DBEUMM21XXX",  # Deutsche Bank Frankfurt
    "SANESMMXXXX",  # Banco Santander Madrid
    "BCHICLMMXXX",  # Banco de Chile Santiago
    "ITAUCLRMXXX",  # Itau Corpbanca Santiago
    "BOFABRSPXXX",  # Bank of America Brazil
]

CURRENCY_PAIRS = [
    ("USD", "EUR", Decimal("0.9250")),
    ("EUR", "USD", Decimal("1.0810")),
    ("USD", "GBP", Decimal("0.7880")),
    ("USD", "CLP", Decimal("945.50")),
    ("USD", "BRL", Decimal("5.4500")),
    ("EUR", "CLP", Decimal("1022.00")),
    ("GBP", "USD", Decimal("1.2690")),
]


class SyntheticFeedGenerator:
    """Generates realistic high-yield cross-border transaction batches with optional chaos injection."""

    def __init__(self, seed: int = 42):
        self.seed = seed
        self._rng = random.Random(seed)

    def generate_batch(
        self,
        count: int = 100,
        batch_date: str = "2026-10-08",
        inject_chaos: bool = False,
        chaos_ratio: float = 0.05,
    ) -> list[RawPaymentEvent]:
        events: list[RawPaymentEvent] = []
        batch_id = f"batch_{batch_date.replace('-', '')}_{self._rng.randint(1000, 9999)}"
        base_dt = datetime.fromisoformat(f"{batch_date}T09:00:00").replace(tzinfo=timezone.utc)

        for i in range(count):
            event_id = str(uuid.UUID(int=self._rng.getrandbits(128)))
            tx_id = f"tx_{self._rng.randint(10000000, 99999999)}"

            # Select pair
            src_curr, tgt_curr, base_fx = self._rng.choice(CURRENCY_PAIRS)
            # Add micro FX spread volatility (-0.5% to +0.5%)
            fx_jitter = Decimal(str(round(self._rng.uniform(-0.005, 0.005), 4)))
            actual_fx = (base_fx * (Decimal("1.0") + fx_jitter)).quantize(Decimal("0.0001"))

            # Pick sender and receiver
            sender = self._rng.choice(MAJOR_BICS)
            receivers = [b for b in MAJOR_BICS if b != sender]
            receiver = self._rng.choice(receivers)

            amount = Decimal(str(round(self._rng.uniform(1000.0, 500000.0), 2)))
            target_amount = (amount * actual_fx).quantize(Decimal("0.01"))
            timestamp = base_dt.replace(
                hour=self._rng.randint(8, 17),
                minute=self._rng.randint(0, 59),
                second=self._rng.randint(0, 59),
            )

            is_chaotic = inject_chaos and (self._rng.random() < chaos_ratio)

            if is_chaotic:
                raw_payload = self._create_chaotic_payload(
                    tx_id=tx_id,
                    sender=sender,
                    receiver=receiver,
                    src_curr=src_curr,
                    tgt_curr=tgt_curr,
                    amount=amount,
                    actual_fx=actual_fx,
                    target_amount=target_amount,
                    timestamp=timestamp,
                )
            else:
                raw_payload = json.dumps({
                    "transaction_id": tx_id,
                    "timestamp_utc": timestamp.isoformat(),
                    "sender_bic": sender,
                    "receiver_bic": receiver,
                    "settlement_currency": src_curr,
                    "settlement_amount": str(amount),
                    "target_currency": tgt_curr,
                    "booked_fx_rate": str(actual_fx),
                    "target_amount": str(target_amount),
                    "status": "SETTLED",
                })

            events.append(
                RawPaymentEvent(
                    event_id=event_id,
                    batch_id=batch_id,
                    ingested_at_utc=datetime.now(timezone.utc),
                    ingestion_date=batch_date,
                    source_feed="SWIFT_FIN",
                    raw_payload=raw_payload,
                )
            )

        return events

    def _create_chaotic_payload(
        self,
        tx_id: str,
        sender: str,
        receiver: str,
        src_curr: str,
        tgt_curr: str,
        amount: Decimal,
        actual_fx: Decimal,
        target_amount: Decimal,
        timestamp: datetime,
    ) -> str:
        chaos_type = self._rng.randint(1, 4)

        if chaos_type == 1:
            # Corrupted currency
            src_curr = "XYZ"
        elif chaos_type == 2:
            # Negative or zero amount
            amount = Decimal("-5000.00")
        elif chaos_type == 3:
            # Invalid short SWIFT BIC
            sender = "INVALID_BIC"
        elif chaos_type == 4:
            # Malformed truncated JSON
            return '{"transaction_id": "' + tx_id + '", "amount": 1000.0, "status": "BROKEN_JSON'

        return json.dumps({
            "transaction_id": tx_id,
            "timestamp_utc": timestamp.isoformat(),
            "sender_bic": sender,
            "receiver_bic": receiver,
            "settlement_currency": src_curr,
            "settlement_amount": str(amount),
            "target_currency": tgt_curr,
            "booked_fx_rate": str(actual_fx),
            "target_amount": str(target_amount),
            "status": "SETTLED",
        })
