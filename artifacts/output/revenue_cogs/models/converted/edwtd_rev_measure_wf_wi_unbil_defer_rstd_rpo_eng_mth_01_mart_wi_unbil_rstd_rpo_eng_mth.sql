{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_unbil_defer_rstd_rpo_eng_mth', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_UNBIL_DEFER_RSTD_RPO_ENG_MTH',
        'target_table': 'WI_UNBIL_RSTD_RPO_ENG_MTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.509073+00:00'
    }
) }}

WITH 

source_wi_defer_rstd_rpo_eng_mth AS (
    SELECT
        processed_fiscal_year_mth,
        fiscal_year_mth,
        product_key,
        sales_territory_key,
        service_flg,
        transaction_type,
        src_entity,
        rpo_flg,
        unbilled_revenue_amt,
        projected_unbill_rev_amt,
        short_term_unbilled_rev_amt,
        long_term_unbilled_rev_amt,
        bk_parent_offer_type_name,
        bk_offer_type_name,
        nonrec_rev_amt,
        short_term_nonrec_rev_amt,
        long_term_nonrec_rev_amt,
        balance_rev_usd_amt,
        projected_balance_rev_usd_amt,
        short_term_bal_rev_usd_amt,
        long_term_bal_rev_rev_usd_amt,
        edw_create_dtm,
        projected_nonrec_rev_usd_amt,
        projected_rev_usd_amt
    FROM {{ source('raw', 'wi_defer_rstd_rpo_eng_mth') }}
),

source_wi_unbil_rstd_rpo_eng_mth AS (
    SELECT
        processed_fiscal_year_mth,
        fiscal_year_mth,
        product_key,
        sales_territory_key,
        service_flg,
        transaction_type,
        src_entity,
        rpo_flg,
        unbilled_revenue_amt,
        projected_unbill_rev_amt,
        short_term_unbilled_rev_amt,
        long_term_unbilled_rev_amt,
        bk_parent_offer_type_name,
        bk_offer_type_name,
        nonrec_rev_amt,
        short_term_nonrec_rev_amt,
        long_term_nonrec_rev_amt,
        balance_rev_usd_amt,
        projected_balance_rev_usd_amt,
        short_term_bal_rev_usd_amt,
        long_term_bal_rev_rev_usd_amt,
        edw_create_dtm,
        projected_nonrec_rev_usd_amt,
        projected_rev_usd_amt
    FROM {{ source('raw', 'wi_unbil_rstd_rpo_eng_mth') }}
),

final AS (
    SELECT
        processed_fiscal_year_mth,
        fiscal_year_mth,
        product_key,
        sales_territory_key,
        service_flg,
        transaction_type,
        src_entity,
        rpo_flg,
        unbilled_revenue_amt,
        projected_unbill_rev_amt,
        short_term_unbilled_rev_amt,
        long_term_unbilled_rev_amt,
        bk_parent_offer_type_name,
        bk_offer_type_name,
        nonrec_rev_amt,
        short_term_nonrec_rev_amt,
        long_term_nonrec_rev_amt,
        balance_rev_usd_amt,
        projected_balance_rev_usd_amt,
        short_term_bal_rev_usd_amt,
        long_term_bal_rev_rev_usd_amt,
        edw_create_dtm,
        projected_nonrec_rev_usd_amt,
        projected_rev_usd_amt
    FROM source_wi_unbil_rstd_rpo_eng_mth
)

SELECT * FROM final