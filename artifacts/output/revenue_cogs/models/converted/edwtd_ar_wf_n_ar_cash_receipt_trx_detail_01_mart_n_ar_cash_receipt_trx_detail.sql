{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_cash_receipt_trx_detail', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_AR_CASH_RECEIPT_TRX_DETAIL',
        'target_table': 'N_AR_CASH_RECEIPT_TRX_DETAIL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.342502+00:00'
    }
) }}

WITH 

source_w_ar_cash_receipt_trx_detail AS (
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
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_cash_receipt_trx_detail') }}
),

final AS (
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
        edw_update_user
    FROM source_w_ar_cash_receipt_trx_detail
)

SELECT * FROM final