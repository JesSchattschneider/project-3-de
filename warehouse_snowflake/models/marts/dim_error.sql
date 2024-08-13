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

select 
    id,
    SITE AS site_id,
    VARIABLE AS variable_id,
    ERROR,
    STATUS_CODE,
    CREATED_AT,
    current_timestamp() as last_updated
from {{ source('proj3_raw', 'lwq_data') }}
where 
    ERROR is not null
    {% if is_incremental() %}
    and CREATED_AT > {{ max_time }}
    {% endif %}