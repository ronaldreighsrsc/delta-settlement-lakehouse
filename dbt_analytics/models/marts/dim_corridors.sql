-- Dimensional table summarizing active B2B cross-border FX corridors.

SELECT
    corridor_id,
    settlement_currency,
    target_currency,
    COUNT(*) AS settled_tx_count,
    SUM(settlement_amount) AS total_source_volume,
    ROUND(AVG(booked_fx_rate), 6) AS avg_booked_fx_rate
FROM {{ ref('stg_settlements') }}
GROUP BY corridor_id, settlement_currency, target_currency

