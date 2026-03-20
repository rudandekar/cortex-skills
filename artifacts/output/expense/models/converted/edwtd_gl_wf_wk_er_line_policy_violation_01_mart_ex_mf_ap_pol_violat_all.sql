{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_er_line_policy_violation', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_ER_LINE_POLICY_VIOLATION',
        'target_table': 'EX_MF_AP_POL_VIOLAT_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.654781+00:00'
    }
) }}

WITH 

source_st_mf_ap_pol_violat_all AS (
    SELECT
        batch_id,
        create_datetime,
        action_code,
        report_header_id,
        distribution_line_number,
        violation_number,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        violation_type,
        allowable_amount,
        func_currency_allowable_amt,
        org_id,
        last_update_login,
        exceeded_amount,
        violation_date,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'st_mf_ap_pol_violat_all') }}
),

source_sm_er_line_policy_violation AS (
    SELECT
        er_line_policy_violation_key,
        sk_report_header_id,
        sk_distribution_line_num_int,
        sk_violation_num_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_er_line_policy_violation') }}
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

transformed_exp_er_line_policy_violation AS (
    SELECT
    violation_type,
    report_header_id,
    distribution_line_number,
    violation_number,
    ss_cd,
    er_line_policy_violation_key,
    violation_date,
    allowable_amount,
    exceeded_amount,
    expense_report_line_key,
    er_line_last_update_dtm,
    source_deleted_flg
    FROM source_n_expense_report_line
),

transformed_exptrans AS (
    SELECT
    batch_id,
    create_datetime,
    action_code,
    report_header_id,
    distribution_line_number,
    violation_number,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    violation_type,
    allowable_amount,
    func_currency_allowable_amt,
    org_id,
    last_update_login,
    exceeded_amount,
    violation_date,
    ges_update_date,
    global_name,
    'RI' AS exception_type
    FROM transformed_exp_er_line_policy_violation
),

final AS (
    SELECT
        batch_id,
        create_datetime,
        action_code,
        report_header_id,
        distribution_line_number,
        violation_number,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        violation_type,
        allowable_amount,
        func_currency_allowable_amt,
        org_id,
        last_update_login,
        exceeded_amount,
        violation_date,
        ges_update_date,
        global_name,
        exception_type
    FROM transformed_exptrans
)

SELECT * FROM final