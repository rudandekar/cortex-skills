{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_journal_entry_line_hfm ', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_JOURNAL_ENTRY_LINE_HFM ',
        'target_table': 'WI_JEL_HFM_BAL_AMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.594125+00:00'
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
    FROM source_n_journal_entry
)

SELECT * FROM final