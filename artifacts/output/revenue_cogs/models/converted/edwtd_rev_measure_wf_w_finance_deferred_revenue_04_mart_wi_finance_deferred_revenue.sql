{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_finance_deferred_revenue', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_FINANCE_DEFERRED_REVENUE',
        'target_table': 'WI_FINANCE_DEFERRED_REVENUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.678986+00:00'
    }
) }}

WITH 

source_wi_dfr_open_period AS (
    SELECT
        fiscal_month_id,
        service_flg,
        open_flag
    FROM {{ source('raw', 'wi_dfr_open_period') }}
),

source_wi_fin_defrev_tba AS (
    SELECT
        processed_fiscal_year_mth_int,
        rev_measurement_type_cd,
        bk_ccrm_profile_id_int,
        bk_deal_id,
        bk_measure_name,
        deferred_rev_usd_amt,
        fiscal_year_month_int,
        sales_territory_key,
        product_key,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        service_flg,
        bk_offer_attri_id_int
    FROM {{ source('raw', 'wi_fin_defrev_tba') }}
),

source_sm_finance_deferred_revenue AS (
    SELECT
        finance_deferred_rev_key,
        bk_prcssd_fiscal_cal_cd,
        bk_prcssd_fiscal_year_num_int,
        bk_prcssd_fiscal_mth_num_int,
        src_entity_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        bk_deal_id,
        bk_ccrm_profile_id_int,
        bk_applied_fiscal_cal_cd,
        bk_applied_fiscal_year_num_int,
        bk_applied_fiscal_mth_num_int,
        rev_measurement_type_cd,
        service_flg,
        dv_top_sku_prdt_key,
        bk_offer_attri_cd,
        sales_order_key,
        bk_offer_attri_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_finance_deferred_revenue') }}
),

source_w_finance_deferred_revenue AS (
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
        action_code,
        dml_type
    FROM {{ source('raw', 'w_finance_deferred_revenue') }}
),

source_wi_finance_deferred_revenue AS (
    SELECT
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
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        bk_offer_attri_id_int
    FROM {{ source('raw', 'wi_finance_deferred_revenue') }}
),

final AS (
    SELECT
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
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        bk_offer_attri_id_int
    FROM source_wi_finance_deferred_revenue
)

SELECT * FROM final