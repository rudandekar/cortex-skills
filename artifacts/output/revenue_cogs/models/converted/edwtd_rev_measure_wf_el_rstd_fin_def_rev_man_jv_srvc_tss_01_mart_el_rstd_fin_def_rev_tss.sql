{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_rstd_fin_def_rev_man_jv_srvc_tss', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_RSTD_FIN_DEF_REV_MAN_JV_SRVC_TSS',
        'target_table': 'EL_RSTD_FIN_DEF_REV_TSS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.612817+00:00'
    }
) }}

WITH 

source_el_rstd_fin_def_rev_tss AS (
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
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM {{ source('raw', 'el_rstd_fin_def_rev_tss') }}
),

final AS (
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
        ar_trx_line_key,
        dv_ar_trx_line_key,
        sales_order_line_key,
        dv_sales_order_line_key
    FROM source_el_rstd_fin_def_rev_tss
)

SELECT * FROM final