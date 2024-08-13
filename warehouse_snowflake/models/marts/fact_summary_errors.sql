{{ config(
    materialized="incremental",
    incremental_strategy="delete+insert",
    depends_on=[
            'MARTS_MARTS.FACT_RECORDS'
        ]
) }}

WITH source_data AS (
    SELECT
        id,
        site_id,
        VARIABLE,
        ERROR,
        CREATED_AT
    FROM {{ source('marts', 'FACT_RECORDS') }}
),

-- Aggregate the data by site and variable, assuming you want to count errors and successes
aggregated_data AS (
    SELECT
        CREATED_AT,
        ERROR,
        COUNT(*) AS count,
    FROM source_data
    GROUP BY CREATED_AT, ERROR
)

SELECT * FROM aggregated_data