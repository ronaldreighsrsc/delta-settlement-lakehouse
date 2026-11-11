-- Singular reconciliation invariant: Gold fct_daily_settlements must reconcile strictly
-- against Silver stg_settlements counts and volume totals per date and currency.

WITH silver_agg AS (
    SELECT
        settlement_date,
        settlement_currency,
        COUNT(*) AS silver_tx_count,
        SUM(settlement_amount) AS silver_total_amount
    FROM {{ ref('stg_settlements') }}
    GROUP BY settlement_date, settlement_currency
),
gold_agg AS (
    SELECT
        settlement_date,
        settlement_currency,
        transaction_count AS gold_tx_count,
        total_settled_amount AS gold_total_amount
    FROM {{ ref('fct_daily_settlements') }}
)
SELECT
    COALESCE(s.settlement_date, g.settlement_date) AS settlement_date,
    COALESCE(s.settlement_currency, g.settlement_currency) AS settlement_currency,
    s.silver_tx_count,
    g.gold_tx_count,
    s.silver_total_amount,
    g.gold_total_amount
FROM silver_agg s
FULL OUTER JOIN gold_agg g
    ON s.settlement_date = g.settlement_date
   AND s.settlement_currency = g.settlement_currency
WHERE COALESCE(s.silver_tx_count, -1) <> COALESCE(g.gold_tx_count, -1)
   OR ABS(COALESCE(s.silver_total_amount, 0) - COALESCE(g.gold_total_amount, 0)) > 0.01

