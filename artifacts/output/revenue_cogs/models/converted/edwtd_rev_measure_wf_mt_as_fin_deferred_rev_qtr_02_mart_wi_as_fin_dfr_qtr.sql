{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_as_fin_deferred_rev_qtr', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_AS_FIN_DEFERRED_REV_QTR',
        'target_table': 'WI_AS_FIN_DFR_QTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.474149+00:00'
    }
) }}

WITH 

source_mt_as_fin_deferred_rev_qtr AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        balance_rev_usd_amt,
        recognized_rev_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_beginning_blnce_rev_usd_amt,
        dv_bridge_balance_rev_usd_amt,
        sk_offer_attribution_id_int,
        product_subscription_flg,
        deferral_rev_usd_amt,
        recognize_or_projected_type_cd,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        method_type,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'mt_as_fin_deferred_rev_qtr') }}
),

source_wi_as_fin_dfr_qtr AS (
    SELECT
        fiscal_year_quarter_number_int,
        rev_measurement_type_cd,
        fiscal_year_month_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        def_ref_amt,
        sk_offer_attribution_id_int,
        product_subscription_flg,
        recognize_or_projected_type_cd,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        method_type,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_as_fin_dfr_qtr') }}
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        rev_measurement_type_cd,
        fiscal_year_month_int,
        dv_measure_name,
        bk_measure_name,
        sales_territory_key,
        product_key,
        src_entity_name,
        bk_deal_id,
        erp_deal_id,
        bk_ccrm_profile_id_int,
        service_flg,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        def_ref_amt,
        sk_offer_attribution_id_int,
        product_subscription_flg,
        recognize_or_projected_type_cd,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        method_type,
        dv_recurring_offer_cd
    FROM source_wi_as_fin_dfr_qtr
)

SELECT * FROM final