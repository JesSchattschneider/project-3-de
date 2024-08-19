-- Combine lwq_data and transfer_table - left join on variable from lwq_data and callname from transfer_table
WITH variables_tbl AS (
    SELECT
        lwq.id,
        lwq.variable,
        lwq.CREATED_AT,
        trans.agency,
        trans.callname,
        {{ dbt_utils.generate_surrogate_key(['id', 'variable']) }} AS id_variable
    FROM 
        {{ source('proj3_raw', 'lwq_data') }} AS lwq
    LEFT JOIN 
        {{ source('proj3_raw', 'transfer_table') }} AS trans
    ON 
        lwq.variable = trans.callname
)

SELECT DISTINCT
    id_variable,
    id,
    variable,
    CREATED_AT,
    agency,
    callname
FROM variables_tbl
