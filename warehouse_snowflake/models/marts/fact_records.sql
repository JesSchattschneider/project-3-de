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
      {{ dbt_utils.generate_surrogate_key(['id']) }} AS id_record,
      lwq.id,
      -- Handle missing values in the 't' column
      CASE 
          WHEN t = 'missing' THEN NULL -- or use a default timestamp value
          ELSE 
              CASE
                  WHEN TRY_TO_TIMESTAMP(lwq.t, 'YYYYMMDDTHH24MISS') IS NOT NULL THEN TRY_TO_TIMESTAMP(t, 'YYYYMMDDTHH24MISS')
                  WHEN TRY_TO_TIMESTAMP(lwq.t, 'YYYY-MM-DDTHH24:MI:SS') IS NOT NULL THEN TRY_TO_TIMESTAMP(t, 'YYYY-MM-DDTHH24:MI:SS')
                  -- Add more formats as needed
                  -- ELSE NULL -- Handle unsupported formats
              END
      END AS datetime_var,
      lwq.ERROR,
      lwq.SITE AS site_id,
      lwq.LAWANAME AS variable,
      lwq.VALUE as RAW_VALUE,
      -- Extract the numeric value after 'than_' and cast it to FLOAT with conditional logic
      CASE
         WHEN lwq.VALUE LIKE '%than_%' THEN
            REGEXP_SUBSTR(lwq.VALUE, '[0-9]+(\.[0-9]+)?$')::FLOAT
          --when value is None, return NULL
          when lwq.VALUE = 'None' then NULL 
          WHEN lwq.VALUE = 'missing' THEN NULL
          WHEN lwq.VALUE = 'NA' THEN NULL
          WHEN lwq.VALUE = '*' THEN NULL
          WHEN lwq.VALUE = '' THEN NULL
         ELSE lwq.VALUE::FLOAT  -- or some other default value
      END AS value_numeric,

      REGEXP_REPLACE(lwq.VALUE, '[0-9]', '') AS censored,
      lwq.URL AS data_url,
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
),

-- Adjust the value based on the censored column
adjusted_data AS (
  SELECT
    *,
    CASE
        WHEN POSITION('less' IN censored) > 0 THEN value_numeric / 2
        WHEN POSITION('greater' IN censored) > 0 THEN value_numeric * 1.1
        ELSE value_numeric
    END AS adjusted_value
  FROM data_tbl
)

SELECT
    ad.*,
    dv.id_variable AS id_variable,
    ds.id_site AS id_site,
    de.id_error AS id_error
FROM adjusted_data ad
INNER JOIN {{ ref('dim_variables') }} dv
  ON ad.id = dv.id

INNER JOIN {{ ref('dim_sites') }} ds
  ON ad.id = ds.id

INNER JOIN {{ ref('dim_error') }} de
  ON ad.id = de.id

