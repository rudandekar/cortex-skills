{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_rstd_unbilled_deferred_rev_rpo_eng', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_RSTD_UNBILLED_DEFERRED_REV_RPO_ENG',
        'target_table': 'MT_RSTD_UNBILLED_DEFERRED_REV_RPO_ENG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.871777+00:00'
    }
) }}

WITH 

source_mt_rstd_unbilled_deferred_rev_rpo_eng AS (
    SELECT
        processed_fiscal_mth,
        fiscal_mth,
        attributed_product_key,
        sales_territory_key,
        service_flg,
        transaction_type_cd,
        src_entity_name,
        rpo_flg,
        unbilled_revenue_usd_amt,
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
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        projected_nonrec_rev_usd_amt,
        projected_rev_usd_amt
    FROM {{ source('raw', 'mt_rstd_unbilled_deferred_rev_rpo_eng') }}
),

final AS (
    SELECT
        processed_fiscal_mth,
        fiscal_mth,
        attributed_product_key,
        sales_territory_key,
        service_flg,
        transaction_type_cd,
        src_entity_name,
        rpo_flg,
        unbilled_revenue_usd_amt,
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
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        projected_nonrec_rev_usd_amt,
        projected_rev_usd_amt
    FROM source_mt_rstd_unbilled_deferred_rev_rpo_eng
)

SELECT * FROM final