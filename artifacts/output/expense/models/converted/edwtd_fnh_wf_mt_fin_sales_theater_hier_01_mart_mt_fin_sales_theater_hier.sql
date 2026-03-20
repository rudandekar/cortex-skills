{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_fin_sales_theater_hier', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_MT_FIN_SALES_THEATER_HIER',
        'target_table': 'MT_FIN_SALES_THEATER_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.898730+00:00'
    }
) }}

WITH 

source_mt_fin_sales_theater_hier AS (
    SELECT
        sales_territory_key,
        level01_theater_name,
        level02_theater_name,
        level03_theater_name,
        level04_theater_name,
        level05_theater_name,
        level06_theater_name,
        level07_theater_name,
        level08_theater_name,
        level09_theater_name,
        level10_theater_name,
        level11_theater_name,
        level12_theater_name,
        level13_theater_name,
        level14_theater_name,
        level15_theater_name,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'mt_fin_sales_theater_hier') }}
),

final AS (
    SELECT
        sales_territory_key,
        level01_theater_name,
        level02_theater_name,
        level03_theater_name,
        level04_theater_name,
        level05_theater_name,
        level06_theater_name,
        level07_theater_name,
        level08_theater_name,
        level09_theater_name,
        level10_theater_name,
        level11_theater_name,
        level12_theater_name,
        level13_theater_name,
        level14_theater_name,
        level15_theater_name,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_mt_fin_sales_theater_hier
)

SELECT * FROM final