{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_vendor_invoice_payment', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_VENDOR_INVOICE_PAYMENT',
        'target_table': 'N_VENDOR_INVOICE_PAYMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.098576+00:00'
    }
) }}

WITH 

source_w_vendor_invoice_payment AS (
    SELECT
        vendor_invoice_payment_key,
        vendor_invoice_key,
        vndr_inv_pymt_instrument_key,
        vndr_inv_pymt_cross_rt,
        vndr_inv_pymt_paid_dt,
        vndr_inv_pymt_num_int,
        vndr_inv_pymt_cross_rate_dt,
        liability_gl_account_key,
        asset_gl_account_key,
        vndr_inv_pymt_amt,
        vndr_inv_pymt_posted_flg,
        fx_loss_gl_account_key,
        fx_gain_gl_account_key,
        discount_taken_functional_amt,
        discount_lost_functional_amt,
        operating_unit_name_cd,
        source_deleted_flg,
        vndr_inv_pymt_hold_flg,
        sk_invoice_payment_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type,
        vndr_inv_payment_created_dtm
    FROM {{ source('raw', 'w_vendor_invoice_payment') }}
),

final AS (
    SELECT
        vendor_invoice_payment_key,
        vendor_invoice_key,
        vndr_inv_pymt_instrument_key,
        vndr_inv_pymt_cross_rt,
        vndr_inv_pymt_paid_dt,
        vndr_inv_pymt_num_int,
        vndr_inv_pymt_cross_rate_dt,
        liability_gl_account_key,
        asset_gl_account_key,
        vndr_inv_pymt_amt,
        vndr_inv_pymt_posted_flg,
        fx_loss_gl_account_key,
        fx_gain_gl_account_key,
        discount_taken_functional_amt,
        discount_lost_functional_amt,
        operating_unit_name_cd,
        source_deleted_flg,
        vndr_inv_pymt_hold_flg,
        sk_invoice_payment_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        vndr_inv_payment_created_dtm,
        reversal_flg
    FROM source_w_vendor_invoice_payment
)

SELECT * FROM final