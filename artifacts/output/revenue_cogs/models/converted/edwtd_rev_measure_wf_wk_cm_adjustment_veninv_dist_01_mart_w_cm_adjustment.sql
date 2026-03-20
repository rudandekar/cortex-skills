{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_cm_adjustment_veninv_dist', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WK_CM_ADJUSTMENT_VENINV_DIST',
        'target_table': 'W_CM_ADJUSTMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.304524+00:00'
    }
) }}

WITH 

source_n_vendor_invoice_distribution AS (
    SELECT
        vendor_invoice_distrib_key,
        bk_inv_distrib_line_num_int,
        bk_vendor_invoice_num,
        ap_vendor_party_key,
        general_ledger_account_key,
        vndr_inv_distrib_acct_dt,
        vndr_inv_distrib_trx_type_cd,
        vndr_inv_distrib_invoiced_qty,
        vndr_inv_distrib_unit_prc_amt,
        vndr_inv_distrib_inv_vrc_amt,
        vndr_inv_distrib_doc_type_cd,
        vndr_inv_distrib_posted_flg,
        vndr_inv_distrib_line_descr,
        vndr_inv_distrib_amt,
        expense_reimbursement_type_cd,
        po_shipment_distribution_key,
        vendor_invoice_key,
        fiscal_year_number_int,
        fiscal_month_number_int,
        expense_report_line_key,
        source_deleted_flg,
        fiscal_calendar_cd,
        sk_invoice_distribution_id_int,
        ss_cd,
        dd_bk_company_cd,
        dd_bk_department_cd,
        dd_bk_financial_account_cd,
        dd_bk_financial_location_cd,
        dd_bk_project_cd,
        dd_bk_subaccount_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        vendor_invoice_line_key,
        bk_item_unit_of_measure_cd,
        dv_trx_fsc_yr_mth_wk_num_int
    FROM {{ source('raw', 'n_vendor_invoice_distribution') }}
),

final AS (
    SELECT
        cm_adjustment_key,
        functional_currency_cd,
        bk_financial_account_cd,
        bill_to_customer_key,
        bk_ar_trx_num,
        ru_cust_purchase_order_num,
        ar_type_cd,
        dv_ar_adj_trx_functional_amt,
        dv_ar_adj_trx_usd_amt,
        gl_dt,
        gl_posted_dt,
        ar_adj_trx_reason_cd,
        vendor_name,
        operating_unit_name_cd,
        cust_trx_type_category_cd,
        fiscal_year_month_num_int,
        bk_company_code,
        bk_set_of_books_key,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        action_code,
        dml_type,
        dv_trx_fsc_yr_mth_wk_num_int
    FROM source_n_vendor_invoice_distribution
)

SELECT * FROM final