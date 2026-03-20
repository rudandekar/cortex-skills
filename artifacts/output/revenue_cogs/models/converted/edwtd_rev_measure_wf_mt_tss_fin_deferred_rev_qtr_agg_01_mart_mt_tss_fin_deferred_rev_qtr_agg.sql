{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_tss_fin_deferred_rev_qtr_agg', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_TSS_FIN_DEFERRED_REV_QTR_AGG',
        'target_table': 'MT_TSS_FIN_DEFERRED_REV_QTR_AGG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.003459+00:00'
    }
) }}

WITH 

source_mt_tss_fin_deferred_rev_qtr AS (
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
        dv_goods_product_key,
        dv_tss_allctn_method_type,
        dv_recurring_offer_cd,
        dv_cx_product
    FROM {{ source('raw', 'mt_tss_fin_deferred_rev_qtr') }}
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        measure_name,
        sales_territory_key,
        product_key,
        src_entity_name,
        deal_id,
        ccrm_profile_id_int,
        service_flg,
        dv_attribution_cd,
        dv_product_key,
        balance_rev_usd_amt_cq,
        balance_rev_usd_amt_pq,
        balance_rev_usd_amt_py,
        recognized_rev_usd_amt_cq,
        recognized_rev_usd_amt_pq,
        recognized_rev_usd_amt_py,
        beginning_blnce_rev_usd_amt_cq,
        beginning_blnce_rev_usd_amt_pq,
        beginning_blnce_rev_usd_amt_py,
        bridge_balance_rev_usd_amt_cq,
        bridge_balance_rev_usd_amt_pq,
        bridge_balance_rev_usd_amt_py,
        product_subscription_flg,
        recognize_or_projected_type_cd,
        goods_product_key,
        tss_allctn_method_type,
        recurring_offer_cd,
        dv_cx_product
    FROM source_mt_tss_fin_deferred_rev_qtr
)

SELECT * FROM final