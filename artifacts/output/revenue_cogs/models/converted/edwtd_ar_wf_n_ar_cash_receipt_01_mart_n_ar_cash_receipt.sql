{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_cash_receipt', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_N_AR_CASH_RECEIPT',
        'target_table': 'N_AR_CASH_RECEIPT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.179879+00:00'
    }
) }}

WITH 

source_w_ar_cash_receipt AS (
    SELECT
        ar_cash_receipt_key,
        cash_receipt_num,
        cash_receipt_dt,
        bk_company_cd,
        bk_set_of_books_key,
        cash_receipt_transactional_amt,
        bk_transactional_currency_cd,
        inv_bill_to_cust_loc_use_key,
        pay_from_customer_account_key,
        sk_cash_receipt_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_cash_receipt') }}
),

final AS (
    SELECT
        ar_cash_receipt_key,
        cash_receipt_num,
        cash_receipt_dt,
        bk_company_cd,
        bk_set_of_books_key,
        cash_receipt_transactional_amt,
        bk_transactional_currency_cd,
        inv_bill_to_cust_loc_use_key,
        pay_from_customer_account_key,
        sk_cash_receipt_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ar_cash_receipt
)

SELECT * FROM final