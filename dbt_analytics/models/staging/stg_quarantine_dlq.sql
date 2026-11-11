-- Staging view over quarantined Dead Letter Queue events.
-- Extracts standardized error taxonomy prefix for operational governance monitoring.

SELECT
    event_id,
    batch_id,
    quarantined_at_utc,
    split_part(rejection_reason, ':', 1) AS error_code,
    rejection_reason,
    raw_payload
FROM {{ source('raw_lakehouse', 'silver_quarantine_dlq') }}

