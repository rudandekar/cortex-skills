{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_finance_deferred_revenue', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_FINANCE_DEFERRED_REVENUE',
        'target_table': 'N_FINANCE_DEFERRED_REVENUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.360973+00:00'
    }
) }}

WITH 

source_n_finance_deferred_revenue AS (
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
        bk_offer_attri_id_int
    FROM {{ source('raw', 'n_finance_deferred_revenue') }}
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
        bk_offer_attri_id_int
    FROM source_n_finance_deferred_revenue
)

SELECT * FROM final