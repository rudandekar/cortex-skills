{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_er_reimbursmnt_type_inv_line', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_ER_REIMBURSMNT_TYPE_INV_LINE',
        'target_table': 'W_ER_REIMBURSMNT_TYPE_INV_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.621022+00:00'
    }
) }}

WITH 

source_n_er_reimbursmnt_type_inv_line AS (
    SELECT
        bk_expense_reimbursmnt_typ_cd,
        expense_report_line_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_er_reimbursmnt_type_inv_line') }}
),

source_n_expense_report_line AS (
    SELECT
        expense_report_line_key,
        bk_expense_report_line_num_int,
        bk_ss_cd,
        er_line_trxl_currency_cd,
        er_line_exchng_rt_effctv_dtm,
        er_line_receipt_required_flg,
        er_line_merchant_name,
        er_line_last_update_dtm,
        er_line_exchange_rate_type_cd,
        er_line_vat_cd,
        er_line_exchange_rt,
        er_line_creation_dtm,
        er_line_crt_wrkr_emp_prty_key,
        er_line_upd_wrkr_emp_prty_key,
        er_line_type_cd,
        er_line_type_lookup_cd,
        er_line_free_meal_cnt,
        er_line_breakfast_included_flg,
        er_line_adjustment_reason_cd,
        er_line_location_descr,
        er_line_tax_code_override_flg,
        er_line_amt_includes_tax_flg,
        er_line_expense_incurred_dtm,
        er_line_item_descr,
        er_line_justification_txt,
        er_line_attendee_cnt,
        er_line_short_payment_flg,
        er_line_transactional_amt,
        er_line_receipt_trxl_amt,
        er_line_receipt_conversion_rt,
        er_line_receipt_verified_flg,
        er_line_receipt_currency_cd,
        sk_report_line_id_int,
        expense_report_key,
        source_deleted_flg,
        er_line_gl_account_key,
        er_line_category_cd,
        dd_bk_company_cd,
        dd_bk_department_cd,
        dd_bk_project_cd,
        dd_bk_financial_account_cd,
        dd_bk_financial_location_cd,
        dd_bk_subaccount_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_expense_report_line') }}
),

final AS (
    SELECT
        bk_expense_reimbursmnt_typ_cd,
        expense_report_line_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM source_n_expense_report_line
)

SELECT * FROM final