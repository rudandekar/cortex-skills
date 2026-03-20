{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_expense_report', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_EXPENSE_REPORT',
        'target_table': 'N_EXPENSE_REPORT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.979920+00:00'
    }
) }}

WITH 

source_w_expense_report AS (
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
        audit_catch_notes_txt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_expense_report') }}
),

final AS (
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
    FROM source_w_expense_report
)

SELECT * FROM final