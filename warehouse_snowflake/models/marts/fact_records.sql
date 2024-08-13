{{ config(
    materialized="incremental",
    incremental_strategy="delete+insert"
) }}

{% set max_time %}
  {% if is_incremental() %}
    (select max(CREATED_AT) from {{ this }})
  {% else %}
    null
  {% endif %}
{% endset %}

SELECT 
    id,
    -- Handle missing values in the 't' column
    CASE 
        WHEN t = 'missing' THEN NULL -- or use a default timestamp value
        ELSE to_timestamp(t)
    END AS datetime_var,
    SITE AS site_id,
    LAWASITEID,
    LAWANAME AS variable,
    ERROR,
    -- Extract numeric value or put non-numeric values into censored column
    CASE 
        WHEN REGEXP_LIKE(VALUE, '^[0-9]+(\.[0-9]+)?$') THEN VALUE::FLOAT
        ELSE NULL
    END AS value_numeric,
    CASE
        WHEN REGEXP_LIKE(VALUE, '^[0-9]+(\.[0-9]+)?$') THEN NULL
        ELSE VALUE
    END AS censored,
    URL,
    CREATED_AT,
    current_timestamp() AS last_updated
FROM {{ source('proj3_raw', 'lwq_data') }}
{% if is_incremental() %}
    WHERE CREATED_AT > {{ max_time }}
{% endif %}