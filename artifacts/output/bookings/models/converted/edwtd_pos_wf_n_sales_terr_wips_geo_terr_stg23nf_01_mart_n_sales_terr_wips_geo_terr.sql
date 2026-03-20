{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_terr_wips_geo_terr_stg23nf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_SALES_TERR_WIPS_GEO_TERR_STG23NF',
        'target_table': 'N_SALES_TERR_WIPS_GEO_TERR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.271976+00:00'
    }
) }}

WITH 

source_st_wips_disti_sales_geo_map AS (
    SELECT
        sales_level3,
        sales_theater_name,
        sales_geo_name,
        created_date,
        last_updated_date
    FROM {{ source('raw', 'st_wips_disti_sales_geo_map') }}
),

final AS (
    SELECT
        sales_territory_key,
        sales_theater_name,
        sales_geo_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_wips_disti_sales_geo_map
)

SELECT * FROM final