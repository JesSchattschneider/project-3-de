{{ config(
    materialized="table",
    depends_on=[
            'MARTS_MARTS.FACT_RECORDS'
        ]
) }}

-- Example SQL to summarize data from fact_records
SELECT
    site_id,
    LAWASITEID,
    variable,
    COUNT(*) AS total_records,
    AVG(VALUE_NUMERIC) AS average_value,
    MAX(VALUE_NUMERIC) AS max_value,
    MAX(CREATED_AT) AS last_updated_at,
    MIN(CREATED_AT) AS first_updated_at
FROM {{ source('marts', 'FACT_RECORDS') }}
GROUP BY site_id, LAWASITEID, variable
