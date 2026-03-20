{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_product_cost_factor', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_PRODUCT_COST_FACTOR',
        'target_table': 'ST_TSS_PRODUCT_COST_FACTOR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.956767+00:00'
    }
) }}

WITH 

source_st_tss_product_cost_factor AS (
    SELECT
        product_key,
        bk_iso_country_cd,
        bk_price_list_name,
        sales_territory_name,
        bkgs_measure_trans_type_code,
        cost_factor_pct,
        country_flg,
        rebate_factor_pct,
        edw_create_dtm,
        edw_upload_dtm,
        edw_upload_user
    FROM {{ source('raw', 'st_tss_product_cost_factor') }}
),

final AS (
    SELECT
        product_key,
        bk_iso_country_cd,
        bk_price_list_name,
        sales_territory_name,
        bkgs_measure_trans_type_code,
        cost_factor_pct,
        country_flg,
        rebate_factor_pct,
        edw_create_dtm,
        edw_upload_dtm,
        edw_upload_user
    FROM source_st_tss_product_cost_factor
)

SELECT * FROM final