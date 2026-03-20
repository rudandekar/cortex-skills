{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ar_trx_line_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_N_AR_TRX_LINE_OOD',
        'target_table': 'N_AR_TRX_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.316946+00:00'
    }
) }}

WITH 

source_w_ar_trx_line AS (
    SELECT
        ar_trx_line_key,
        ar_trx_key,
        bk_adjd_ar_trx_line_key,
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_number,
        bk_ar_trx_type_code,
        bk_ar_trx_line_type,
        bk_ar_trx_line_number_int,
        ar_trx_line_reason_code,
        bk_saf_set_of_books_key,
        bk_saf_company_code,
        bk_saf_id_int,
        applied_transaction_type,
        ar_trx_adjustment_line_role,
        ar_trx_line_creation_datetime,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        ar_transaction_line_descr,
        ru_xaas_prepayment_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ar_trx_line') }}
),

final AS (
    SELECT
        ar_trx_line_key,
        ar_trx_key,
        bk_adjd_ar_trx_line_key,
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_number,
        bk_ar_trx_type_code,
        bk_ar_trx_line_type,
        bk_ar_trx_line_number_int,
        ar_trx_line_reason_code,
        bk_saf_set_of_books_key,
        bk_saf_company_code,
        bk_saf_id_int,
        applied_transaction_type,
        ar_trx_adjustment_line_role,
        ar_trx_line_creation_datetime,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        ar_transaction_line_descr,
        ru_xaas_prepayment_flg
    FROM source_w_ar_trx_line
)

SELECT * FROM final