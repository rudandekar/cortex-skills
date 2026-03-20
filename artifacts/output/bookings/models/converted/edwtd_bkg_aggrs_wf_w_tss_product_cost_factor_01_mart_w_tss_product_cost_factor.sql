{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_tss_product_cost_factor', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_W_TSS_PRODUCT_COST_FACTOR',
        'target_table': 'W_TSS_PRODUCT_COST_FACTOR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.726995+00:00'
    }
) }}

WITH 

source_w_tss_product_cost_factor AS (
    SELECT
        tss_country_factor_key,
        product_key,
        bk_price_list_name,
        bk_iso_country_cd,
        cost_factor_pct,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        src_lst_updtd_wrkr_prty_key,
        country_flg,
        sales_territory_name,
        bkgs_measure_trans_type_code,
        rebate_factor_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_tss_product_cost_factor') }}
),

final AS (
    SELECT
        tss_country_factor_key,
        product_key,
        bk_price_list_name,
        bk_iso_country_cd,
        cost_factor_pct,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        src_lst_updtd_wrkr_prty_key,
        country_flg,
        sales_territory_name,
        bkgs_measure_trans_type_code,
        rebate_factor_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_tss_product_cost_factor
)

SELECT * FROM final