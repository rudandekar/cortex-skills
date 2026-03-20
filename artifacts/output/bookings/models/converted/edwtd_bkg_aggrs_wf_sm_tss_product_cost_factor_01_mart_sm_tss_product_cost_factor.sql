{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_tss_product_cost_factor', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_SM_TSS_PRODUCT_COST_FACTOR',
        'target_table': 'SM_TSS_PRODUCT_COST_FACTOR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.541893+00:00'
    }
) }}

WITH 

source_sm_tss_product_cost_factor AS (
    SELECT
        tss_country_factor_key,
        product_key,
        bk_iso_country_cd,
        bk_price_list_name,
        dv_sales_territory_descr,
        bkgs_measure_trans_type_code,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_tss_product_cost_factor') }}
),

final AS (
    SELECT
        tss_country_factor_key,
        product_key,
        bk_iso_country_cd,
        bk_price_list_name,
        sales_territory_name,
        bkgs_measure_trans_type_code,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_tss_product_cost_factor
)

SELECT * FROM final