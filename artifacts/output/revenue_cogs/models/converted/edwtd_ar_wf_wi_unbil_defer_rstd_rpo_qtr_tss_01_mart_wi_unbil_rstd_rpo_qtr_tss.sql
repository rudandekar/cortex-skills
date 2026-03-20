{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_unbil_defer_rstd_rpo_qtr_tss', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_UNBIL_DEFER_RSTD_RPO_QTR_TSS',
        'target_table': 'WI_UNBIL_RSTD_RPO_QTR_TSS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.847458+00:00'
    }
) }}

WITH 

source_wi_unbil_rstd_rpo_qtr_tss AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        sales_territory_key,
        attributed_product_key,
        service_flg,
        bk_parent_offer_type_name,
        bk_offer_type_name,
        transaction_type,
        src_entity,
        rpo_flg,
        dv_gross_unb_curr_qtr_amt,
        dv_gross_unb_prev_qtr_amt,
        dv_gross_unb_prev_year_qtr_amt,
        dv_deferred_rev_bal_curr_qtr_amt,
        dv_deferred_rev_bal_prev_qtr_amt,
        dv_deferred_rev_bal_prev_year_qtr_amt,
        dv_nonrec_rev_bal_curr_qtr_amt,
        dv_nonrec_rev_bal_prev_qtr_amt,
        dv_nonrec_rev_bal_prev_year_qtr_amt,
        edw_create_dtm,
        goods_product_key,
        dv_tss_alctn_mthd_type,
        dv_pid_alloc_pct,
        product_key,
        dv_cx_product
    FROM {{ source('raw', 'wi_unbil_rstd_rpo_qtr_tss') }}
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        sales_territory_key,
        attributed_product_key,
        service_flg,
        bk_parent_offer_type_name,
        bk_offer_type_name,
        transaction_type,
        src_entity,
        rpo_flg,
        dv_gross_unb_curr_qtr_amt,
        dv_gross_unb_prev_qtr_amt,
        dv_gross_unb_prev_year_qtr_amt,
        dv_deferred_rev_bal_curr_qtr_amt,
        dv_deferred_rev_bal_prev_qtr_amt,
        dv_deferred_rev_bal_prev_year_qtr_amt,
        dv_nonrec_rev_bal_curr_qtr_amt,
        dv_nonrec_rev_bal_prev_qtr_amt,
        dv_nonrec_rev_bal_prev_year_qtr_amt,
        edw_create_dtm,
        goods_product_key,
        dv_tss_alctn_mthd_type,
        dv_pid_alloc_pct,
        product_key,
        dv_cx_product
    FROM source_wi_unbil_rstd_rpo_qtr_tss
)

SELECT * FROM final