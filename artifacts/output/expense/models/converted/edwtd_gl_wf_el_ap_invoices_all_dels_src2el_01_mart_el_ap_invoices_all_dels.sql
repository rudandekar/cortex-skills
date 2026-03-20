{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ap_invoices_all_dels_src2el', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_AP_INVOICES_ALL_DELS_SRC2EL',
        'target_table': 'EL_AP_INVOICES_ALL_DELS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.594951+00:00'
    }
) }}

WITH 

source_mf_ap_invoices_all_dels AS (
    SELECT
        accts_pay_code_combination_id,
        amount_applicable_to_discount,
        amount_paid,
        amt_due_ccard_company,
        amt_due_employee,
        approval_description,
        approval_iteration,
        approval_ready_flag,
        approval_status,
        approved_amount,
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
        authorized_by,
        auto_tax_calc_flag,
        award_id,
        awt_flag,
        awt_group_id,
        base_amount,
        batch_id,
        cancelled_amount,
        cancelled_by,
        cancelled_date,
        created_by,
        creation_date,
        description,
        discount_amount_taken,
        doc_category_code,
        doc_sequence_id,
        doc_sequence_value,
        earliest_settlement_date,
        exchange_date,
        exchange_rate,
        exchange_rate_type,
        exclusive_payment_flag,
        expenditure_item_date,
        expenditure_organization_id,
        expenditure_type,
        freight_amount,
        ges_update_date,
        ges_delete_date,
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
        gl_date,
        goods_received_date,
        invoice_amount,
        invoice_currency_code,
        invoice_date,
        invoice_distribution_total,
        invoice_id,
        invoice_num,
        invoice_received_date,
        invoice_type_lookup_code,
        last_updated_by,
        last_update_date,
        last_update_login,
        mrc_base_amount,
        mrc_exchange_date,
        mrc_exchange_rate,
        mrc_exchange_rate_type,
        org_id,
        original_prepayment_amount,
        paid_on_behalf_employee_id,
        payment_amount_total,
        payment_cross_rate,
        payment_cross_rate_date,
        payment_cross_rate_type,
        payment_currency_code,
        payment_method_lookup_code,
        payment_status_flag,
        pay_curr_invoice_amount,
        pay_group_lookup_code,
        pa_default_dist_ccid,
        pa_quantity,
        posting_status,
        po_header_id,
        prepay_flag,
        pre_withholding_amount,
        project_accounting_context,
        project_id,
        recurring_payment_id,
        reference_1,
        reference_2,
        requester_id,
        set_of_books_id,
        source,
        task_id,
        tax_amount,
        temp_cancelled_amount,
        terms_date,
        terms_id,
        ussgl_transaction_code,
        ussgl_trx_code_context,
        vat_code,
        vendor_id,
        vendor_prepay_amount,
        vendor_site_id,
        voucher_num,
        wfapproval_status
    FROM {{ source('raw', 'mf_ap_invoices_all_dels') }}
),

transformed_exptrans AS (
    SELECT
    ges_delete_date,
    global_name,
    batch_id,
    invoice_id,
    org_id,
    po_header_id,
    set_of_books_id,
    terms_id,
    vendor_id,
    vendor_site_id,
    lkp_ges_delete_date,
    lkp_global_name,
    lkp_invoice_batch_id,
    lkp_invoice_id,
    lkp_org_id,
    lkp_po_header_id,
    lkp_set_of_books_id,
    lkp_terms_id,
    lkp_vendor_id,
    lkp_vendor_site_id
    FROM source_mf_ap_invoices_all_dels
),

routed_rtrtrans AS (
    SELECT 
        *,
        CASE 
            WHEN TRUE THEN 'INPUT'
            WHEN TRUE THEN 'INSERT'
            WHEN TRUE THEN 'DEFAULT1'
            ELSE 'DEFAULT'
        END AS _router_group
    FROM transformed_exptrans
),

update_strategy_ins_updtrans AS (
    SELECT
        *,
        CASE 
            WHEN DD_INSERT = 0 THEN 'INSERT'
            WHEN DD_INSERT = 1 THEN 'UPDATE'
            WHEN DD_INSERT = 2 THEN 'DELETE'
            ELSE 'REJECT'
        END AS _dbt_change_type,
        CURRENT_TIMESTAMP() AS _dbt_updated_at
    FROM routed_rtrtrans
    WHERE DD_INSERT != 3
),

lookup_lkptrans1 AS (
    SELECT
        a.*,
        b.*
    FROM update_strategy_ins_updtrans a
    LEFT JOIN {{ source('raw', 'el_ap_invoices_all_dels') }} b
        ON a.in_global_name = b.in_global_name
),

transformed_exptrans1 AS (
    SELECT
    ges_delete_date,
    global_name,
    batch_id,
    invoice_id,
    org_id,
    po_header_id,
    set_of_books_id,
    terms_id,
    vendor_id,
    vendor_site_id,
    TO_INTEGER(INVOICE_ID) AS out_invoice_id
    FROM lookup_lkptrans1
),

final AS (
    SELECT
        ges_delete_date,
        global_name,
        invoice_batch_id,
        invoice_id,
        org_id,
        po_header_id,
        set_of_books_id,
        terms_id,
        vendor_id,
        vendor_site_id
    FROM transformed_exptrans1
)

SELECT * FROM final