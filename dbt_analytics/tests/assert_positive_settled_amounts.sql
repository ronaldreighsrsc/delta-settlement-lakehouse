-- Singular invariant: settled financial amounts must remain strictly positive.

SELECT
    transaction_id,
    settlement_currency,
    settlement_amount
FROM {{ ref('stg_settlements') }}
WHERE settlement_amount <= 0

