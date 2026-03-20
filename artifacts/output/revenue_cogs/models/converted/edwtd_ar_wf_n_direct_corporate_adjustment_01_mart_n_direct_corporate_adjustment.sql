{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_direct_corporate_adjustment', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_DIRECT_CORPORATE_ADJUSTMENT',
        'target_table': 'N_DIRECT_CORPORATE_ADJUSTMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.028729+00:00'
    }
) }}

WITH 

source_w_direct_corporate_adjustment AS (
    SELECT
        direct_corp_adjustment_key,
        sales_order_line_key,
        dv_transaction_dt,
        transaction_dtm,
        adjusment_cogs_usd_amt,
        adjusment_cogs_functional_amt,
        adjustment_revenue_usd_amt,
        adjustment_revenue_functnl_amt,
        bk_direct_corp_adj_type_cd,
        direct_corp_adjustment_qty,
        transactional_currency_cd,
        bk_company_cd,
        set_of_books_key,
        sk_adjustment_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        sales_order_key,
        bk_offer_atrbtn_id_int,
        bk_atrbtn_source_sys_cd,
        dv_atrbtn_parent_slsord_ln_key,
        attribution_cd,
        product_key,
        ru_attribution_pct,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_direct_corporate_adjustment') }}
),

final AS (
    SELECT
        direct_corp_adjustment_key,
        sales_order_line_key,
        dv_transaction_dt,
        transaction_dtm,
        adjusment_cogs_usd_amt,
        adjusment_cogs_functional_amt,
        adjustment_revenue_usd_amt,
        adjustment_revenue_functnl_amt,
        bk_direct_corp_adj_type_cd,
        direct_corp_adjustment_qty,
        transactional_currency_cd,
        bk_company_cd,
        set_of_books_key,
        sk_adjustment_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        sales_order_key,
        bk_offer_atrbtn_id_int,
        bk_atrbtn_source_sys_cd,
        dv_atrbtn_parent_slsord_ln_key,
        attribution_cd,
        product_key,
        ru_attribution_pct,
        rol_trx_line_gl_distri_key
    FROM source_w_direct_corporate_adjustment
)

SELECT * FROM final