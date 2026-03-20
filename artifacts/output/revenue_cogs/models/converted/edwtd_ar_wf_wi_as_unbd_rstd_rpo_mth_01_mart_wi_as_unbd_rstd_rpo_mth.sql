{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_unbd_defer_rstd_rpo_mth', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_AS_UNBD_DEFER_RSTD_RPO_MTH',
        'target_table': 'WI_AS_UNBD_RSTD_RPO_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.617882+00:00'
    }
) }}

WITH 

source_wi_as_unbd_rstd_rpo_mth AS (
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
        edw_create_dtm,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        method_type,
        dv_pid_alloc_pct
    FROM {{ source('raw', 'wi_as_unbd_rstd_rpo_mth') }}
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
        edw_create_dtm,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        method_type
    FROM source_wi_as_unbd_rstd_rpo_mth
)

SELECT * FROM final