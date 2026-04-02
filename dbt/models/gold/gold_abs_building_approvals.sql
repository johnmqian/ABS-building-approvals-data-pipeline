{{ config(materialized='table')}}

SELECT
    time_period,
    region_code,
    region_label,
    building_type_label,
    measure_label,
    sector_label,
    work_type_label,
    SUM(obs_value) as obs_value_total,
    ingestion_date
FROM {{ source('SILVER_ABS_BUILDING_APPROVALS', 'SILVER_ABS_BUILDING_APPROVALS')}}
-- FROM ABS_BUILDING_APPROVALS_DATABASE.ABS_BUILDING_APPROVALS.SILVER_ABS_BUILDING_APPROVALS
GROUP BY
    time_period,
    region_code,
    region_label,
    building_type_label,
    measure_label,
    sector_label,
    work_type_label,
    ingestion_date