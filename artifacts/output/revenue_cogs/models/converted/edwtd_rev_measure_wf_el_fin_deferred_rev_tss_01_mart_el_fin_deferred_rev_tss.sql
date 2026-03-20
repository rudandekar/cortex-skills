{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_fin_deferred_rev_tss', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_FIN_DEFERRED_REV_TSS',
        'target_table': 'EL_FIN_DEFERRED_REV_TSS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.736177+00:00'
    }
) }}

WITH 

source_wi_finance_deferred_rev_tss AS (
    SELECT
        finance_deferred_rev_key,
        bk_prcssd_fiscal_cal_cd,
        bk_prcssd_fiscal_year_num_int,
        bk_prcssd_fiscal_mth_num_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        bk_applied_fiscal_cal_cd,
        bk_applied_fiscal_year_num_int,
        bk_applied_fiscal_mth_num_int,
        src_entity_name,
        bk_deal_id,
        bk_ccrm_profile_id_int,
        rev_measurement_type_cd,
        service_flg,
        deferred_rev_usd_amt,
        src_created_user_name,
        src_created_dtm,
        dv_src_created_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_top_sku_prdt_key,
        bk_offer_attri_cd,
        sales_order_key,
        bk_offer_attri_id_int,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'wi_finance_deferred_rev_tss') }}
),

final AS (
    SELECT
        finance_deferred_rev_key,
        bk_prcssd_fiscal_cal_cd,
        bk_prcssd_fiscal_year_num_int,
        bk_prcssd_fiscal_mth_num_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        bk_applied_fiscal_cal_cd,
        bk_applied_fiscal_year_num_int,
        bk_applied_fiscal_mth_num_int,
        src_entity_name,
        bk_deal_id,
        bk_ccrm_profile_id_int,
        rev_measurement_type_cd,
        service_flg,
        deferred_rev_usd_amt,
        src_created_user_name,
        src_created_dtm,
        dv_src_created_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_top_sku_prdt_key,
        bk_offer_attri_cd,
        sales_order_key,
        bk_offer_attri_id_int,
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM source_wi_finance_deferred_rev_tss
)

SELECT * FROM final