{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_journal_entry_line_hfm ', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_JOURNAL_ENTRY_LINE_HFM ',
        'target_table': 'N_JOURNAL_ENTRY_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.024017+00:00'
    }
) }}

WITH 

source_wi_jel_hfm_bal_amt AS (
    SELECT
        bk_journal_entry_line_num_int,
        bk_journal_entry_number_int,
        set_of_books_key,
        bk_company_code,
        ru_dv_usd_credit_amt,
        ru_dv_usd_debit_amt,
        ru_functional_credit_amt,
        ru_functional_debit_amt,
        ru_transactional_credit_amt,
        ru_transactional_debit_amt,
        dd_bk_financial_location_cd,
        dd_financial_account_cd,
        dd_bk_dept_cd,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        dv_fiscal_year_month_num_int,
        fiscal_month_start_date,
        debit_credit_entry_type,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int
    FROM {{ source('raw', 'wi_jel_hfm_bal_amt') }}
),

source_n_hfm_account_balance AS (
    SELECT
        bk_financial_account_cd,
        bk_financial_location_cd,
        bk_fiscal_calendar_cd,
        bk_fiscal_year_num_int,
        bk_fiscal_month_num_int,
        bk_company_cd,
        bk_intercompany_company_cd,
        bk_finance_report_line_item_cd,
        bk_hfm_reported_department_cd,
        bk_rollup_category_cd,
        bk_view_level_cd,
        department_type,
        consolidated_balance_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_bk_hfm_department_cd,
        ru_bk_department_code,
        source_deleted_flg
    FROM {{ source('raw', 'n_hfm_account_balance') }}
),

source_n_journal_entry AS (
    SELECT
        bk_journal_entry_number_int,
        set_of_books_key,
        accrual_reversal_flg,
        bk_journal_entry_category_name,
        bk_journal_entry_source_name,
        currency_conversion_dt,
        effective_dt_calendar_cd,
        effective_dt_fscl_year_num_int,
        effective_dt_month_num_int,
        entered_by_erp_user_name,
        bk_journal_entry_batch_id_int,
        journal_entry_descr,
        journal_entry_name,
        journal_entry_reversal_role,
        ovrd_trx_fnc_curr_cnv_rt,
        parent_role,
        posted_dtm,
        ru_parent_jrnl_entry_num_int,
        ru_reversal_jrnl_entry_num_int,
        ss_cd,
        tax_status_flg,
        transactional_currency_cd,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM {{ source('raw', 'n_journal_entry') }}
),

final AS (
    SELECT
        bk_journal_entry_line_num_int,
        bk_journal_entry_number_int,
        set_of_books_key,
        bk_company_code,
        bk_functional_currency_code,
        debit_credit_entry_type,
        effective_dt,
        effective_dt_calendar_cd,
        effective_dt_fscl_year_num_int,
        effective_dt_month_num_int,
        general_ledger_account_key,
        intercompany_role,
        journal_entry_line_type,
        journal_entry_line_type_cd,
        ovrd_trx_fnc_curr_cnv_rt,
        ru_dv_usd_credit_amt,
        ru_dv_usd_debit_amt,
        ru_functional_credit_amt,
        ru_functional_debit_amt,
        ru_offset_line_gla_company_cd,
        ru_offset_line_gla_dept_cd,
        ru_offset_line_gla_fin_acct_cd,
        ru_offset_line_gla_fin_loc_cd,
        ru_offset_line_gla_project_cd,
        ru_offset_line_gla_subacct_cd,
        ru_transactional_credit_amt,
        ru_transactional_debit_amt,
        ss_cd,
        dd_bk_subaccount_locality_int,
        dd_bk_fin_acct_locality_int,
        dd_bk_project_locality_int,
        dd_bk_financial_location_cd,
        dd_financial_account_cd,
        dd_bk_subaccount_cd,
        dd_bk_dept_cd,
        dd_bk_project_cd,
        dd_financial_company_mapped_cd,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        dv_fiscal_year_month_num_int,
        journal_entry_line_descr,
        ru_vendor_invoice_payment_key,
        ep_process_summary_id,
        jel_crtd_by_csco_wrkr_pty_key,
        jrnl_entry_line_create_dtm,
        dv_jrnl_entry_line_create_dt,
        ru_sales_order_key,
        ru_sales_order_line_key,
        ru_product_unit_cost_trans_amt
    FROM source_n_journal_entry
)

SELECT * FROM final