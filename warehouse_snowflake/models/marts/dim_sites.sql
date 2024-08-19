WITH sites_tbl AS (
    SELECT
        lwq.id,
        lwq.variable,
        site_table.*,  -- This selects all columns from site_table
        {{ dbt_utils.generate_surrogate_key(['id', 'SITEID', 'COUNCILSITEID']) }} AS id_site
    FROM 
        {{ source('proj3_raw', 'lwq_data') }} AS lwq
    LEFT JOIN 
        {{ source('proj3_raw', 'lwq_wfs_table_latest') }} AS site_table
    ON 
        lwq.site = site_table.COUNCILSITEID
)

select * from sites_tbl