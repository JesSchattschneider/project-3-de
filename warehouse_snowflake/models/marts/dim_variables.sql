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

-- Combine lwq_data and transfer_table - left join on variable from lwq_data and callname from transfer_table
WITH variables_tbl AS (
    SELECT
        lwq.id,
        lwq.variable,
        lwq.CREATED_AT,
        trans.agency,
        trans.callname
    FROM 
        {{ source('proj3_raw', 'lwq_data') }} AS lwq
    LEFT JOIN 
        {{ source('proj3_raw', 'transfer_table') }} AS trans
    ON 
        lwq.variable = trans.callname
    
    {% if is_incremental() %}
    WHERE lwq.CREATED_AT > {{ max_time }}
    {% endif %}
)

SELECT * FROM variables_tbl
