{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ap_invoice_distrib_dels_src2el', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_AP_INVOICE_DISTRIB_DELS_SRC2EL',
        'target_table': 'EL_AP_INVOICE_DISTRIB_DELS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.655530+00:00'
    }
) }}

WITH 

source_mf_ap_invoice_distri_all_dels AS (
    SELECT
        accounting_date,
        accounting_event_id,
        accrual_posted_flag,
        accts_pay_code_combination_id,
        adjustment_reason,
        amount,
        amount_encumbered,
        amount_includes_tax_flag,
        amount_to_post,
        assets_addition_flag,
        assets_tracking_flag,
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
        award_id,
        awt_flag,
        awt_gross_amount,
        awt_group_id,
        awt_invoice_id,
        awt_invoice_payment_id,
        awt_origin_group_id,
        awt_tax_rate_id,
        awt_withheld_amt,
        base_amount,
        base_amount_encumbered,
        base_amount_to_post,
        base_invoice_price_variance,
        base_quantity_variance,
        batch_id,
        cancellation_flag,
        cash_je_batch_id,
        cash_posted_flag,
        cc_reversal_flag,
        company_prepaid_invoice_id,
        country_of_supply,
        created_by,
        creation_date,
        credit_card_trx_id,
        daily_amount,
        description,
        distribution_line_number,
        dist_code_combination_id,
        dist_match_type,
        earliest_settlement_date,
        encumbered_flag,
        end_expense_date,
        exchange_date,
        exchange_rate,
        exchange_rate_type,
        exchange_rate_variance,
        expenditure_item_date,
        expenditure_organization_id,
        expenditure_type,
        expense_group,
        final_match_flag,
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
        gms_burdenable_raw_cost,
        income_tax_region,
        inventory_transfer_status,
        invoice_distribution_id,
        invoice_id,
        invoice_includes_prepay_flag,
        invoice_price_variance,
        jebe_vat_alloc_flag,
        je_batch_id,
        justification,
        last_updated_by,
        last_update_date,
        last_update_login,
        line_group_number,
        line_type_lookup_code,
        matched_uom_lookup_code,
        match_status_flag,
        merchant_document_number,
        merchant_name,
        merchant_reference,
        merchant_taxpayer_id,
        merchant_tax_reg_number,
        mrc_base_amount,
        mrc_base_inv_price_variance,
        mrc_dist_code_combination_id,
        mrc_exchange_date,
        mrc_exchange_rate,
        mrc_exchange_rate_type,
        mrc_exchange_rate_variance,
        mrc_rate_var_ccid,
        mrc_receipt_conversion_rate,
        org_id,
        other_invoice_id,
        packet_id,
        parent_invoice_id,
        parent_reversal_id,
        pa_addition_flag,
        pa_cc_ar_invoice_id,
        pa_cc_ar_invoice_line_num,
        pa_cc_processed_code,
        pa_cmt_xface_flag,
        pa_quantity,
        period_name,
        posted_amount,
        posted_base_amount,
        posted_flag,
        po_distribution_id,
        prepay_amount_remaining,
        prepay_distribution_id,
        prepay_tax_parent_id,
        price_adjustment_flag,
        price_correct_inv_id,
        price_correct_qty,
        price_var_code_combination_id,
        program_application_id,
        program_id,
        program_update_date,
        project_accounting_context,
        project_id,
        quantity_invoiced,
        quantity_unencumbered,
        quantity_variance,
        rate_var_code_combination_id,
        rcv_transaction_id,
        receipt_conversion_rate,
        receipt_currency_amount,
        receipt_currency_code,
        receipt_missing_flag,
        receipt_required_flag,
        receipt_verified_flag,
        reference_1,
        reference_2,
        request_id,
        req_distribution_id,
        reversal_flag,
        set_of_books_id,
        start_expense_date,
        stat_amount,
        task_id,
        tax_calculated_flag,
        tax_code_id,
        tax_code_override_flag,
        tax_recoverable_flag,
        tax_recovery_override_flag,
        tax_recovery_rate,
        type_1099,
        unit_price,
        upgrade_base_posted_amt,
        upgrade_posted_amt,
        ussgl_transaction_code,
        ussgl_trx_code_context,
        vat_code,
        web_parameter_id
    FROM {{ source('raw', 'mf_ap_invoice_distri_all_dels') }}
),

transformed_exptrans AS (
    SELECT
    distribution_line_number,
    ges_delete_date,
    global_name,
    batch_id,
    invoice_distribution_id,
    invoice_id,
    org_id,
    set_of_books_id,
    lkp_distribution_line_number,
    lkp_ges_delete_date,
    lkp_global_name,
    lkp_invoice_batch_id,
    lkp_invoice_distribution_id,
    lkp_invoice_id,
    lkp_org_id,
    lkp_set_of_books_id
    FROM source_mf_ap_invoice_distri_all_dels
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

lookup_lkptrans AS (
    SELECT
        a.*,
        b.*
    FROM update_strategy_ins_updtrans a
    LEFT JOIN {{ source('raw', 'el_ap_invoice_distrib_dels') }} b
        ON a.in_distribution_line_number = b.in_distribution_line_number
),

transformed_exptrans1 AS (
    SELECT
    distribution_line_number,
    ges_delete_date,
    global_name,
    batch_id,
    invoice_distribution_id,
    invoice_id,
    org_id,
    set_of_books_id,
    TO_INTEGER(DISTRIBUTION_LINE_NUMBER) AS out_distribution_line_number,
    TO_INTEGER(INVOICE_ID) AS out_invoice_id
    FROM lookup_lkptrans
),

final AS (
    SELECT
        distribution_line_number,
        ges_delete_date,
        global_name,
        invoice_batch_id,
        invoice_distribution_id,
        invoice_id,
        org_id,
        set_of_books_id
    FROM transformed_exptrans1
)

SELECT * FROM final