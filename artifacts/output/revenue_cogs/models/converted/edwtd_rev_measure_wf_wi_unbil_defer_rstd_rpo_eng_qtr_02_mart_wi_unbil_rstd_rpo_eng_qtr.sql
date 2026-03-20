{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_unbil_defer_rstd_rpo_eng_qtr', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_UNBIL_DEFER_RSTD_RPO_ENG_QTR',
        'target_table': 'WI_UNBIL_RSTD_RPO_ENG_QTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.402862+00:00'
    }
) }}

WITH 

source_wi_unbil_rstd_rpo_eng_qtr AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        sales_territory_key,
        product_key,
        service_flg,
        bk_parent_offer_type_name,
        bk_offer_type_name,
        transaction_type,
        src_entity,
        rpo_flg,
        gross_unbilled_amt_cq,
        gross_unbilled_amt_pq,
        gross_unbilled_amt_pyq,
        deferred_revenue_bal_amt_cq,
        deferred_revenue_bal_amt_pq,
        deferred_revenue_bal_amt_pyq,
        nonrec_rev_bal_amt_cq,
        nonrec_rev_bal_amt_pq,
        nonrec_rev_bal_amt_pyq,
        edw_create_dtm
    FROM {{ source('raw', 'wi_unbil_rstd_rpo_eng_qtr') }}
),

source_wi_defer_rstd_rpo_eng_qtr AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        sales_territory_key,
        product_key,
        service_flg,
        bk_parent_offer_type_name,
        bk_offer_type_name,
        transaction_type,
        src_entity,
        rpo_flg,
        gross_unbilled_amt_cq,
        gross_unbilled_amt_pq,
        gross_unbilled_amt_pyq,
        deferred_revenue_bal_amt_cq,
        deferred_revenue_bal_amt_pq,
        deferred_revenue_bal_amt_pyq,
        nonrec_rev_bal_amt_cq,
        nonrec_rev_bal_amt_pq,
        nonrec_rev_bal_amt_pyq,
        edw_create_dtm
    FROM {{ source('raw', 'wi_defer_rstd_rpo_eng_qtr') }}
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        sales_territory_key,
        product_key,
        service_flg,
        bk_parent_offer_type_name,
        bk_offer_type_name,
        transaction_type,
        src_entity,
        rpo_flg,
        gross_unbilled_amt_cq,
        gross_unbilled_amt_pq,
        gross_unbilled_amt_pyq,
        deferred_revenue_bal_amt_cq,
        deferred_revenue_bal_amt_pq,
        deferred_revenue_bal_amt_pyq,
        nonrec_rev_bal_amt_cq,
        nonrec_rev_bal_amt_pq,
        nonrec_rev_bal_amt_pyq,
        edw_create_dtm
    FROM source_wi_defer_rstd_rpo_eng_qtr
)

SELECT * FROM final