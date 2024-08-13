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

-- to do: wide to long format
select 
    *
from {{ source('proj3_raw', 'lwq_metadata') }}
where 
    {% if is_incremental() %}
    and CREATED_AT > {{ max_time }}
    {% endif %}