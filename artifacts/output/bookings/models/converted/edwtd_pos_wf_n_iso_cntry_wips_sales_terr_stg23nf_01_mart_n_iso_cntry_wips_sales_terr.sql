{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iso_cntry_wips_sales_terr_stg23nf', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_ISO_CNTRY_WIPS_SALES_TERR_STG23NF',
        'target_table': 'N_ISO_CNTRY_WIPS_SALES_TERR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.668721+00:00'
    }
) }}

WITH 

source_st_wips_disti_sale_territory AS (
    SELECT
        country,
        sales_territory,
        last_updated_date
    FROM {{ source('raw', 'st_wips_disti_sale_territory') }}
),

final AS (
    SELECT
        bk_iso_country_cd,
        wips_sales_territory_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_wips_disti_sale_territory
)

SELECT * FROM final