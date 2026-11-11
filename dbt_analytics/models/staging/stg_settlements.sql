-- Staging view over validated Delta Lake Silver settlements.
-- Enforces fixed-point numeric precision and derives corridor & 15 bps clearing fee attributes.

SELECT
    transaction_id,
    batch_id,
    timestamp_utc,
    settlement_date,
    settlement_year_month,
    sender_bic,
    receiver_bic,
    settlement_currency,
    target_currency,
    settlement_currency || '-' || target_currency AS corridor_id,
    CAST(settlement_amount AS DECIMAL(18, 4)) AS settlement_amount,
    CAST(booked_fx_rate AS DECIMAL(18, 6)) AS booked_fx_rate,
    CAST(target_amount AS DECIMAL(18, 4)) AS target_amount,
    ROUND(CAST(settlement_amount AS DECIMAL(18, 4)) * 0.0015, 4) AS estimated_fee_amount,
    status,
    reconciliation_hash
FROM {{ source('raw_lakehouse', 'silver_settlements') }}

