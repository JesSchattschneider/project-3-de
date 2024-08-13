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

-- Combine lwq_data and lwq_wfs_table_latest - left join on site from lwq_data and COUNCILSITEID from lwq_wfs_table_latest
WITH sites_tbl AS (
    SELECT
        lwq.id,
        lwq.variable,
        site_table.*  -- This selects all columns from site_table
    FROM 
        {{ source('proj3_raw', 'lwq_data') }} AS lwq
    LEFT JOIN 
        {{ source('proj3_raw', 'lwq_wfs_table_latest') }} AS site_table
    ON 
        lwq.site = site_table.COUNCILSITEID
    
    {% if is_incremental() %}
    WHERE lwq.CREATED_AT > {{ max_time }}
    {% endif %}
)

SELECT * FROM sites_tbl
