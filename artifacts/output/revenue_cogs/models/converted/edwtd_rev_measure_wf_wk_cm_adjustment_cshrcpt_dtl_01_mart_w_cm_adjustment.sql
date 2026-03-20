{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_cm_adjustment_cshrcpt_dtl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WK_CM_ADJUSTMENT_CSHRCPT_DTL',
        'target_table': 'W_CM_ADJUSTMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.698491+00:00'
    }
) }}

WITH 

source_n_ar_cash_receipt_trx_detail AS (
    SELECT
        ar_cash_receipt_trx_detail_key,
        ar_cash_receipt_key,
        gl_posted_dt,
        gl_dtm,
        ar_gl_account_key,
        ar_cash_reciept_trx_detail_typ,
        ar_cash_receipt_trx_dr_cr_typ,
        earned_discount_role,
        unearned_discount_role,
        sk_ar_receivble_applctn_id_int,
        ss_cd,
        ru_applied_ar_trx_key,
        ru_ar_trx_applied_cr_trxl_amt,
        ru_ar_trx_applied_dr_trxl_amt,
        ru_unernd_disc_gl_account_key,
        ru_unernd_discount_dr_cr_typ,
        ru_unernd_disc_takn_trx_cr_amt,
        ru_unernd_disc_takn_trx_dr_amt,
        ru_earned_disc_gl_account_key,
        ru_earned_discount_dr_cr_typ,
        ru_ernd_disc_taken_trxl_cr_amt,
        ru_ernd_disc_taken_trxl_dr_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_trx_fsc_yr_mth_wk_num_int
    FROM {{ source('raw', 'n_ar_cash_receipt_trx_detail') }}
),

final AS (
    SELECT
        cm_adjustment_key,
        functional_currency_cd,
        bk_financial_account_cd,
        bill_to_customer_key,
        bk_ar_trx_num,
        ru_cust_purchase_order_num,
        ar_type_cd,
        dv_ar_adj_trx_functional_amt,
        dv_ar_adj_trx_usd_amt,
        gl_dt,
        gl_posted_dt,
        ar_adj_trx_reason_cd,
        vendor_name,
        operating_unit_name_cd,
        cust_trx_type_category_cd,
        fiscal_year_month_num_int,
        bk_company_code,
        bk_set_of_books_key,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        action_code,
        dml_type,
        dv_trx_fsc_yr_mth_wk_num_int
    FROM source_n_ar_cash_receipt_trx_detail
)

SELECT * FROM final