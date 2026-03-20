{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_as_ub_defer_rev_rpo_qtr', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_MT_AS_UB_DEFER_REV_RPO_QTR',
        'target_table': 'MT_AS_UB_DEFER_REV_RPO_QTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.262274+00:00'
    }
) }}

WITH 

source_mt_as_ub_defer_rev_rpo_qtr AS (
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
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        method_type
    FROM {{ source('raw', 'mt_as_ub_defer_rev_rpo_qtr') }}
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
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        dv_cx_product,
        method_type
    FROM source_mt_as_ub_defer_rev_rpo_qtr
)

SELECT * FROM final