"""Unit tests for the synthetic B2B payment feed and chaos generator."""

import json
import pytest

from delta_settlement_lakehouse.engines.chaos_generator import SyntheticFeedGenerator


def test_generator_deterministic_output():
    gen1 = SyntheticFeedGenerator(seed=123)
    gen2 = SyntheticFeedGenerator(seed=123)

    batch1 = gen1.generate_batch(count=20, batch_date="2026-10-08", inject_chaos=False)
    batch2 = gen2.generate_batch(count=20, batch_date="2026-10-08", inject_chaos=False)

    assert len(batch1) == 20
    assert len(batch2) == 20
    assert batch1[0].raw_payload == batch2[0].raw_payload
    assert batch1[0].event_id == batch2[0].event_id


def test_generator_clean_batch_structure():
    gen = SyntheticFeedGenerator(seed=42)
    batch = gen.generate_batch(count=10, batch_date="2026-10-08", inject_chaos=False)

    for event in batch:
        assert event.ingestion_date == "2026-10-08"
        assert event.source_feed == "SWIFT_FIN"
        payload = json.loads(event.raw_payload)
        assert "transaction_id" in payload
        assert "sender_bic" in payload
        assert "receiver_bic" in payload
        assert "settlement_currency" in payload
        assert float(payload["settlement_amount"]) > 0


def test_generator_with_chaos_injection():
    gen = SyntheticFeedGenerator(seed=999)
    # Generate 100 events with 20% chaos to guarantee anomalous records
    batch = gen.generate_batch(count=100, batch_date="2026-10-08", inject_chaos=True, chaos_ratio=0.20)

    anomalies_detected = 0
    for event in batch:
        try:
            payload = json.loads(event.raw_payload)
            curr = payload.get("settlement_currency", "")
            amt = float(payload.get("settlement_amount", 1.0))
            bic = payload.get("sender_bic", "")
            if curr == "XYZ" or amt <= 0 or len(bic) < 8:
                anomalies_detected += 1
        except Exception:
            # Corrupted non-JSON string
            anomalies_detected += 1

    assert anomalies_detected > 0, "Chaos mode should inject anomalous/corrupted events"
