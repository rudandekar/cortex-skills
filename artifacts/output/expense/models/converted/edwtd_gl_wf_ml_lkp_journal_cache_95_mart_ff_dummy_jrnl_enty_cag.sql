{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ml_lkp_el_fnd_user_cache', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ML_LKP_EL_FND_USER_CACHE',
        'target_table': 'FF_DUMMY_JRNL_ENTY_CAG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.099599+00:00'
    }
) }}

WITH 

source_el_iby_payments_all AS (
    SELECT
        payment_id,
        payment_method_code,
        payment_status,
        payment_amount,
        payment_currency_code,
        internal_bank_account_id,
        org_id,
        creation_date,
        last_update_date,
        payment_date,
        external_bank_account_id,
        global_name,
        ges_update_date,
        create_datetime
    FROM {{ source('raw', 'el_iby_payments_all') }}
),

source_n_ap_bank_account AS (
    SELECT
        ap_bank_account_key,
        ap_bank_branch_key,
        bk_bank_account_num,
        bank_account_type_cd,
        bank_account_name,
        cisco_bank_account_type_cd,
        source_deleted_flg,
        sk_bank_account_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ss_table_cd
    FROM {{ source('raw', 'n_ap_bank_account') }}
),

source_n_set_of_books AS (
    SELECT
        set_of_books_key,
        allow_intercompany_post_flag,
        bk_chart_of_accounts_id_int,
        bk_set_of_books_name,
        functional_currency_code,
        set_of_books_description,
        set_of_books_short_name,
        sk_set_of_books_id_int,
        ss_code,
        edw_update_user,
        edw_create_user,
        edw_create_datetime,
        edw_update_datetime,
        multireporting_currncy_type_cd,
        push_to_glbl_consol_books_flg,
        parent_set_of_books_key
    FROM {{ source('raw', 'n_set_of_books') }}
),

source_n_journal_entry_category AS (
    SELECT
        bk_journal_entry_category_name,
        journal_entry_category_descr,
        ss_cd,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'n_journal_entry_category') }}
),

source_n_expense_report AS (
    SELECT
        er_functional_currency_cd,
        er_creation_dtm,
        er_submitted_dtm,
        er_status_cd,
        er_multiple_currency_flg,
        er_voucher_dtm,
        er_return_instruction_txt,
        er_return_reason_cd,
        er_receipts_received_dtm,
        bk_expense_report_num,
        er_last_update_dtm,
        er_filing_num,
        er_audit_cd,
        er_crt_csco_wrkr_emp_party_key,
        er_operating_unit_name_cd,
        sk_report_header_id_int,
        bk_ss_cd,
        expense_report_key,
        er_purpose_descr,
        er_ln_audt_aprvd_wrkr_prty_key,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        er_for_cisco_wrkr_emp_prty_key,
        meeting_id,
        src_rptd_auditor_name,
        expense_template_cd,
        receipts_status_cd,
        reasonable_cause_delay_descr,
        vendor_pymnt_reject_reason_cd,
        original_submit_dt,
        expense_src_type_cd,
        pending_your_resolution_cd,
        audit_catch_category_name,
        audit_catch_notes_txt
    FROM {{ source('raw', 'n_expense_report') }}
),

source_ev_el_gl_periods AS (
    SELECT
        global_name,
        period_name,
        period_year,
        period_num
    FROM {{ source('raw', 'ev_el_gl_periods') }}
),

source_n_ap_vendor_invoice_batch AS (
    SELECT
        ap_vendor_invoice_batch_key,
        bk_apvi_batch_name,
        bk_operating_unit_name_cd,
        apvi_control_invoice_cnt,
        apvi_control_invoice_total_amt,
        apvi_batch_dtm,
        source_deleted_flg,
        sk_batch_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_ap_vendor_invoice_batch') }}
),

source_n_journal_entry_source AS (
    SELECT
        bk_journal_entry_source_name,
        journal_entry_source_descr,
        user_defined_name,
        ss_cd,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'n_journal_entry_source') }}
),

source_el_fnd_user AS (
    SELECT
        created_by,
        creation_date,
        description,
        email_address,
        employee_id,
        end_date,
        gcn_code_combination_id,
        global_name,
        ges_update_date,
        last_update_date,
        last_updated_by,
        start_date,
        supplier_id,
        user_id,
        user_name,
        cms_replication_date,
        cms_replication_number
    FROM {{ source('raw', 'el_fnd_user') }}
),

final AS (
    SELECT
        dummy1,
        dummy2
    FROM source_el_fnd_user
)

SELECT * FROM final