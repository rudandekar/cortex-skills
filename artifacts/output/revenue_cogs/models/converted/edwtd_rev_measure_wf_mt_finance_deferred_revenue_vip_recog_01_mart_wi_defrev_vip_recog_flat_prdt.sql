{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_finance_deferred_revenue_vip_recog', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_FINANCE_DEFERRED_REVENUE_VIP_RECOG',
        'target_table': 'WI_DEFREV_VIP_RECOG_FLAT_PRDT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.335061+00:00'
    }
) }}

WITH 

source_wi_defrev_vip_recog_flat_prdt AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        sk_offer_attribution_id_int,
        product_subscription_flg,
        dv_beginning_blnce_rev_usd_amt,
        deferral_rev_usd_amt,
        recognize_or_projected_type_cd
    FROM {{ source('raw', 'wi_defrev_vip_recog_flat_prdt') }}
),

source_wi_defrev_vip_recog_vertical_prdt AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        rev_measurement_type_cd,
        service_flg,
        deferred_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        sk_offer_attribution_id_int,
        product_subscription_flg,
        recognize_or_projected_type_cd
    FROM {{ source('raw', 'wi_defrev_vip_recog_vertical_prdt') }}
),

source_wi_defrev_vip_recog AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        rev_measurement_type_cd,
        service_flg,
        deferred_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        sk_offer_attribution_id_int
    FROM {{ source('raw', 'wi_defrev_vip_recog') }}
),

source_mt_finance_deferred_revenue AS (
    SELECT
        processed_fiscal_year_mth_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        recognize_or_projected_type_cd,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        dv_beginning_blnce_rev_usd_amt,
        dv_bridge_balance_rev_usd_amt,
        remaining_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        pob_type_cd,
        product_subscription_flg,
        deferral_rev_usd_amt,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'mt_finance_deferred_revenue') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth_int,
        bk_measure_name,
        sales_territory_key,
        product_key,
        fiscal_year_month_int,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        recognized_rev_usd_amt,
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        projected_balance_rev_usd_amt,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        xcat_flg,
        recurring_offer_flg,
        bk_offer_type_name,
        ela_flg,
        sk_offer_attribution_id_int,
        product_subscription_flg,
        dv_beginning_blnce_rev_usd_amt,
        deferral_rev_usd_amt,
        recognize_or_projected_type_cd
    FROM source_mt_finance_deferred_revenue
)

SELECT * FROM final