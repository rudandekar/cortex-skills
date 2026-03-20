{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_tss_ub_deferred_rev_rpo_tss', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_MT_TSS_UB_DEFERRED_REV_RPO_TSS',
        'target_table': 'MT_TSS_UB_DEFERRED_REV_RPO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.826341+00:00'
    }
) }}

WITH 

source_mt_tss_ub_deferred_rev_rpo AS (
    SELECT
        processed_fiscal_mth,
        fiscal_mth,
        attributed_product_key,
        sales_territory_key,
        service_flg,
        transaction_type,
        src_entity,
        rpo_flg,
        unbilled_revenue_amt,
        dv_projected_unbill_rev_amt,
        dv_short_term_unbilled_rev_amt,
        dv_long_term_unbilled_rev_amt,
        bk_parent_offer_type_name,
        bk_offer_type_name,
        dv_nonrec_rev_amt,
        dv_short_term_nonrec_rev_amt,
        dv_long_term_nonrec_rev_amt,
        balance_rev_usd_amt,
        projected_balance_rev_usd_amt,
        dv_short_term_bal_rev_usd_amt,
        dv_long_term_bal_rev_rev_usd_amt,
        goods_product_key,
        dv_tss_alctn_mthd_type,
        dv_pid_alloc_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        product_key,
        dv_cx_product
    FROM {{ source('raw', 'mt_tss_ub_deferred_rev_rpo') }}
),

final AS (
    SELECT
        processed_fiscal_mth,
        fiscal_mth,
        attributed_product_key,
        sales_territory_key,
        service_flg,
        transaction_type,
        src_entity,
        rpo_flg,
        unbilled_revenue_amt,
        dv_projected_unbill_rev_amt,
        dv_short_term_unbilled_rev_amt,
        dv_long_term_unbilled_rev_amt,
        bk_parent_offer_type_name,
        bk_offer_type_name,
        dv_nonrec_rev_amt,
        dv_short_term_nonrec_rev_amt,
        dv_long_term_nonrec_rev_amt,
        balance_rev_usd_amt,
        projected_balance_rev_usd_amt,
        dv_short_term_bal_rev_usd_amt,
        dv_long_term_bal_rev_rev_usd_amt,
        goods_product_key,
        dv_tss_alctn_mthd_type,
        dv_pid_alloc_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        product_key,
        dv_cx_product
    FROM source_mt_tss_ub_deferred_rev_rpo
)

SELECT * FROM final