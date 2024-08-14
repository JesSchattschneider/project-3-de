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

WITH data_tbl AS (
  SELECT 
      lwq.id,
      -- Handle missing values in the 't' column
      CASE 
          WHEN t = 'missing' THEN NULL -- or use a default timestamp value
          ELSE 
              CASE
                  WHEN TRY_TO_TIMESTAMP(lwq.t, 'YYYYMMDDTHH24MISS') IS NOT NULL THEN TRY_TO_TIMESTAMP(t, 'YYYYMMDDTHH24MISS')
                  WHEN TRY_TO_TIMESTAMP(lwq.t, 'YYYY-MM-DDTHH24:MI:SS') IS NOT NULL THEN TRY_TO_TIMESTAMP(t, 'YYYY-MM-DDTHH24:MI:SS')
                  -- -- Add more formats as needed
                  -- ELSE NULL -- Handle unsupported formats
              END
      END AS datetime_var,
      lwq.ERROR,
      lwq.SITE AS site_id,
      lwq.LAWANAME AS variable,
      lwq.VALUE as RAW_VALUE,
      -- Extract numeric value or put non-numeric values into censored column
      CASE 
          WHEN REGEXP_LIKE(lwq.VALUE, '^[0-9]+(\.[0-9]+)?$') THEN VALUE::FLOAT  -- Extract numeric values
          ELSE NULL
      END AS value_numeric,
      REGEXP_REPLACE(lwq.VALUE, '[0-9]', '') AS censored,
      lwq.URL,
      lwq.CREATED_AT AS data_created_at,
      current_timestamp() AS last_updated,
      trans.*
    FROM 
        {{ source('proj3_raw', 'lwq_data') }} AS lwq
    LEFT JOIN 
        {{ source('proj3_raw', 'lwq_wfs_table_latest') }} AS trans
    ON 
        lwq.site = trans.COUNCILSITEID
    
    {% if is_incremental() %}
    WHERE lwq.CREATED_AT > {{ max_time }}
    {% endif %}
)

SELECT * FROM data_tbl
