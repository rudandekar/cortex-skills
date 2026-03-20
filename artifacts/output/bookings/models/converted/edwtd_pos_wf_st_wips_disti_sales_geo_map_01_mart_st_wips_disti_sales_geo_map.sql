{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_wips_disti_sales_geo_map', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_WIPS_DISTI_SALES_GEO_MAP',
        'target_table': 'ST_WIPS_DISTI_SALES_GEO_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.724790+00:00'
    }
) }}

WITH 

source_ff_wips_disti_sales_geo_map_intf AS (
    SELECT
        sales_level3,
        disti_sales_theatre,
        disti_sales_geo,
        created_date,
        last_updated_date
    FROM {{ source('raw', 'ff_wips_disti_sales_geo_map_intf') }}
),

final AS (
    SELECT
        sales_level3,
        sales_theater_name,
        sales_geo_name,
        created_date,
        last_updated_date
    FROM source_ff_wips_disti_sales_geo_map_intf
)

SELECT * FROM final