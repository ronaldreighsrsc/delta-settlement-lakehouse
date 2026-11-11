-- Fact table capturing reconciled daily B2B clearing volumes and 15 bps fee yields.

SELECT
    settlement_date,
    settlement_currency,
    SUM(settlement_amount) AS total_settled_amount,
    COUNT(*) AS transaction_count,
    SUM(estimated_fee_amount) AS total_fees_collected,
    ROUND(AVG(booked_fx_rate), 6) AS avg_fx_rate,
    MIN(settlement_amount) AS min_settled_amount,
    MAX(settlement_amount) AS max_settled_amount
FROM {{ ref('stg_settlements') }}
GROUP BY settlement_date, settlement_currency

