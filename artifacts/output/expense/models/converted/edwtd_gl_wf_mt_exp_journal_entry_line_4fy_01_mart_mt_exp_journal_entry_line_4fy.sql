{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_exp_journal_entry_line_4fy', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_MT_EXP_JOURNAL_ENTRY_LINE_4FY',
        'target_table': 'MT_EXP_JOURNAL_ENTRY_LINE_4FY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.850476+00:00'
    }
) }}

WITH 

source_n_journal_entry_line AS (
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
        ru_vendor_invoice_distrib_key,
        ru_vendor_invoice_payment_key
    FROM {{ source('raw', 'n_journal_entry_line') }}
),

final AS (
    SELECT
        bk_company_cd,
        bk_department_cd,
        bk_financial_account_cd,
        bk_subaccount_cd,
        bk_project_cd,
        dv_fiscal_year_month_int,
        net_usd_amt,
        net_functional_amt,
        net_transactional_amt,
        net_usd_actuals_at_bud_rt_amt
    FROM source_n_journal_entry_line
)

SELECT * FROM final