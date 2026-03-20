{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_finance_def_rev_trx_man_jv_srvc', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_FINANCE_DEF_REV_TRX_MAN_JV_SRVC',
        'target_table': 'MT_FINANCE_DEF_REV_TRX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.693840+00:00'
    }
) }}

WITH 

source_mt_finance_def_rev_trx AS (
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
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        ar_trx_key,
        ru_bk_product_subgroup_id,
        dv_recurring_offer_cd,
        dv_corporate_revenue_flg,
        ar_trx_line_key,
        net_price_flg
    FROM {{ source('raw', 'mt_finance_def_rev_trx') }}
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
        balance_rev_usd_amt,
        projected_rev_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sales_order_key,
        dv_attribution_cd,
        dv_product_key,
        ar_trx_key,
        ru_bk_product_subgroup_id,
        dv_recurring_offer_cd,
        dv_corporate_revenue_flg,
        ar_trx_line_key,
        net_price_flg
    FROM source_mt_finance_def_rev_trx
)

SELECT * FROM final