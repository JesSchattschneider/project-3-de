{{ config(
    materialized="table",
    depends_on=[
            'MARTS_MARTS.FACT_RECORDS',
            'MARTS_MARTS.LWQ_WFS_TABLE_LATEST'
        ]
) }}

SELECT
    fr.SITEID,
    fr.LAWASITEID,
    fr.COUNCILSITEID,
    fr.variable,
    fr.latitude,
    fr.longitude,
    fr.COUNCIL,
    COUNT(*) AS total_records,
    AVG(fr.VALUE_NUMERIC) AS average_value,
    MAX(fr.VALUE_NUMERIC) AS max_value,
    MIN(fr.VALUE_NUMERIC) AS min_value,
    MAX(fr.CREATED_AT) AS last_updated_at,
    MIN(fr.CREATED_AT) AS first_updated_at
FROM {{ source('marts', 'FACT_RECORDS') }} fr
WHERE fr.VALUE_NUMERIC IS NOT NULL
AND EXISTS (
    SELECT 1
    FROM {{ source('proj3_raw', 'lwq_wfs_table_latest') }} lwq
    WHERE fr.SITEID = lwq.SITEID
    AND fr.LAWASITEID = lwq.LAWASITEID
    AND fr.COUNCILSITEID = lwq.COUNCILSITEID
    AND fr.COUNCIL = lwq.COUNCIL
)
GROUP BY fr.SITEID, fr.LAWASITEID, fr.COUNCILSITEID, fr.COUNCIL, fr.latitude, fr.longitude, fr.variable
