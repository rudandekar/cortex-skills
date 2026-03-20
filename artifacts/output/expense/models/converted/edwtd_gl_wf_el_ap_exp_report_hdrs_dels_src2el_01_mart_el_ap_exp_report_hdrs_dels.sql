{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ap_exp_report_hdrs_dels_src2el', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_AP_EXP_REPORT_HDRS_DELS_SRC2EL',
        'target_table': 'EL_AP_EXP_REPORT_HDRS_DELS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.732625+00:00'
    }
) }}

WITH 

source_mf_ap_exp_report_hdrs_dels AS (
    SELECT
        accounting_date,
        accts_pay_code_combination_id,
        advance_invoice_to_apply,
        amt_due_ccard_company,
        amt_due_employee,
        apply_advances_default,
        attribute1,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute_category,
        audit_code,
        awt_group_id,
        bothpay_parent_id,
        core_wf_status_flag,
        created_by,
        creation_date,
        default_currency_code,
        default_exchange_date,
        default_exchange_rate,
        default_exchange_rate_type,
        default_receipt_currency_code,
        description,
        doc_category_code,
        employee_ccid,
        employee_id,
        expense_check_address_flag,
        expense_current_approver_id,
        expense_last_status_date,
        expense_report_id,
        expense_status_code,
        flex_concatenated,
        ges_delete_date,
        ges_update_date,
        global_attribute1,
        global_attribute10,
        global_attribute11,
        global_attribute12,
        global_attribute13,
        global_attribute14,
        global_attribute15,
        global_attribute16,
        global_attribute17,
        global_attribute18,
        global_attribute19,
        global_attribute2,
        global_attribute20,
        global_attribute3,
        global_attribute4,
        global_attribute5,
        global_attribute6,
        global_attribute7,
        global_attribute8,
        global_attribute9,
        global_attribute_category,
        global_name,
        holding_report_header_id,
        hold_lookup_code,
        invoice_num,
        last_audited_by,
        last_updated_by,
        last_update_date,
        last_update_login,
        maximum_amount_to_apply,
        multiple_currencies_flag,
        org_id,
        override_approver_id,
        override_approver_name,
        paid_on_behalf_employee_id,
        payment_cross_rate,
        payment_cross_rate_date,
        payment_cross_rate_type,
        payment_currency_code,
        prepay_apply_amount,
        prepay_apply_flag,
        prepay_dist_num,
        prepay_gl_date,
        prepay_num,
        purgeable_flag,
        receipts_received_date,
        receipts_status,
        reference_1,
        reference_2,
        reject_code,
        report_filing_number,
        report_header_id,
        report_submitted_date,
        return_instruction,
        return_reason_code,
        set_of_books_id,
        shortpay_parent_id,
        source,
        total,
        ussgl_transaction_code,
        ussgl_trx_code_context,
        vendor_id,
        vendor_site_id,
        voucher_num,
        vouchno,
        week_end_date,
        workflow_approved_flag
    FROM {{ source('raw', 'mf_ap_exp_report_hdrs_dels') }}
),

lookup_lkp_el_ap_exp_report_hdrs_dels AS (
    SELECT
        a.*,
        b.*
    FROM source_mf_ap_exp_report_hdrs_dels a
    LEFT JOIN {{ source('raw', 'el_ap_exp_report_hdrs_dels') }} b
        ON a.in_global_name = b.in_global_name
),

transformed_exp_mf_ap_exp_report_hdrs_dels AS (
    SELECT
    audit_code,
    created_by,
    creation_date,
    default_currency_code,
    description,
    expense_status_code,
    global_name,
    invoice_num,
    last_audited_by,
    last_update_date,
    multiple_currencies_flag,
    org_id,
    receipts_received_date,
    report_filing_number,
    report_header_id,
    report_submitted_date,
    return_instruction,
    return_reason_code,
    vendor_id,
    vendor_site_id,
    TO_DECIMAL(REPORT_HEADER_ID) AS out_report_header_id
    FROM lookup_lkp_el_ap_exp_report_hdrs_dels
),

transformed_exp_el_ap_exp_report_hdrs_dels AS (
    SELECT
    audit_code,
    created_by,
    creation_date,
    default_currency_code,
    description,
    expense_status_code,
    global_name,
    invoice_num,
    last_audited_by,
    last_update_date,
    multiple_currencies_flag,
    org_id,
    receipts_received_date,
    report_filing_number,
    report_header_id,
    report_submitted_date,
    return_instruction,
    return_reason_code,
    vendor_id,
    vendor_site_id,
    lkp_global_name,
    lkp_report_header_id
    FROM transformed_exp_mf_ap_exp_report_hdrs_dels
),

update_strategy_ins_upd_el_ap_exp_report_hdrs_dels AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM transformed_exp_el_ap_exp_report_hdrs_dels
    WHERE DD_INSERT != 3
),

routed_rtr_el_ap_exp_report_hdrs_dels AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM update_strategy_ins_upd_el_ap_exp_report_hdrs_dels
),

final AS (
    SELECT
        audit_code,
        created_by,
        creation_date,
        default_currency_code,
        description,
        expense_status_code,
        global_name,
        invoice_num,
        last_audited_by,
        last_update_date,
        multiple_currencies_flag,
        org_id,
        receipts_received_date,
        report_filing_number,
        report_header_id,
        report_submitted_date,
        return_instruction,
        return_reason_code,
        vendor_id,
        vendor_site_id
    FROM routed_rtr_el_ap_exp_report_hdrs_dels
)

SELECT * FROM final