{{ config(
    materialized="incremental",
    incremental_strategy="delete+insert"
) }}

WITH source_data AS (
    SELECT
        id,
        site_id,
        COUNCIL,
        VARIABLE,
        ERROR,
        DATETIME_VAR,
        COUNCILSITEID,
        -- CREATED_AT,
        CREATED_AT :: DATE AS created_at_date
    FROM {{ ref('fact_records') }}
),

-- Aggregate the data by site and variable, assuming you want to count errors and successes
aggregated_data AS (
    SELECT
        created_at_date,
        COUNCIL,
        ERROR,
        COUNT(*) AS error_count,
        -- combine all unique VARIABLE in a single column
        ARRAY_AGG(DISTINCT VARIABLE) AS variables,
        -- get the number of unique COUNCILSITEID
        COUNT(DISTINCT COUNCILSITEID) AS council_count,
        MIN(DATETIME_VAR),
        MAX(DATETIME_VAR)
    FROM source_data
    GROUP BY created_at_date, ERROR, COUNCIL
)

SELECT * FROM aggregated_data