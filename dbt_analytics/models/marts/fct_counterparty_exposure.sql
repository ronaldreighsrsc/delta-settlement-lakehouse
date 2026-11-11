-- Fact table quantifying bilateral SWIFT BIC bank settlement exposures.

SELECT
    sender_bic,
    receiver_bic,
    settlement_currency,
    COUNT(*) AS transaction_count,
    SUM(settlement_amount) AS net_volume,
    ROUND(AVG(settlement_amount), 4) AS avg_ticket_amount
FROM {{ ref('stg_settlements') }}
GROUP BY sender_bic, receiver_bic, settlement_currency

