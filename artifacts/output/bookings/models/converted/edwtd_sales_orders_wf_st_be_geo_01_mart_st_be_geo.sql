{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_be_geo', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_BE_GEO',
        'target_table': 'ST_BE_GEO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.643357+00:00'
    }
) }}

WITH 

source_ff_be_geo AS (
    SELECT
        be_geo_id,
        be_geo_name,
        be_id,
        geo_type,
        geo_cd,
        locked_flag,
        src_cd,
        reg_flag,
        dp_flag,
        updated_ts,
        added_ts
    FROM {{ source('raw', 'ff_be_geo') }}
),

final AS (
    SELECT
        be_geo_id,
        be_geo_name,
        be_id,
        geo_type,
        geo_cd,
        locked_flag,
        src_cd,
        reg_flag,
        dp_flag,
        updated_ts,
        added_ts
    FROM source_ff_be_geo
)

SELECT * FROM final