{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_rstd_unbilled_defer_rev_rpo_eng_qtr', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_RSTD_UNBILLED_DEFER_REV_RPO_ENG_QTR',
        'target_table': 'MT_RSTD_UNBILLED_DEFER_REV_RPO_ENG_QTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.333430+00:00'
    }
) }}

WITH 

source_mt_rstd_unbilled_defer_rev_rpo_eng_qtr AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        sales_territory_key,
        attributed_product_key,
        service_flg,
        bk_parent_offer_type_name,
        bk_offer_type_name,
        transaction_type_cd,
        src_entity_name,
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
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_rstd_unbilled_defer_rev_rpo_eng_qtr') }}
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
        transaction_type_cd,
        src_entity_name,
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
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_rstd_unbilled_defer_rev_rpo_eng_qtr
)

SELECT * FROM final