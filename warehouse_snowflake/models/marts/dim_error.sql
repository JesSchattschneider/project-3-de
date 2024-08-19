WITH error_tbl AS (
    SELECT
      {{ dbt_utils.generate_surrogate_key(['id', 'ERROR']) }} as id_error,
      id,
      SITE AS site_id,
      VARIABLE AS variable_id,
      ERROR,
      STATUS_CODE,
      CREATED_AT,
      current_timestamp() as last_updated
    FROM 
        {{ source('proj3_raw', 'lwq_data') }} AS lwq
)

SELECT DISTINCT
    id_error,
    id,
    site_id,
    variable_id,
    ERROR,
    STATUS_CODE,
    CREATED_AT,
    last_updated
FROM error_tbl
