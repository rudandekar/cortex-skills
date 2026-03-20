{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ncr_rev_trx_cg', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_NCR_REV_TRX_CG',
        'target_table': 'WI_NCR_REV_GET_ACCT_RULE_CG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.145900+00:00'
    }
) }}

WITH 

source_wi_ncr_rev_sls_cre_flg_cg AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_type_short_code,
        context,
        source_type,
        line_id,
        metrix_flag,
        sales_credit_flag
    FROM {{ source('raw', 'wi_ncr_rev_sls_cre_flg_cg') }}
),

source_ex_ncr_rev_trx_cur AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_key,
        ar_trx_line_key,
        ss_code,
        ar_posting_control_key,
        general_ledger_account_key,
        account_set_flag,
        account_class_code,
        gl_datetime
    FROM {{ source('raw', 'ex_ncr_rev_trx_cur') }}
),

source_wi_ncr_rev_trx_cur AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_key,
        ar_trx_line_key,
        ar_trx_type_short_code,
        ru_bk_adjd_ar_trx_key,
        bk_adjd_ar_trx_line_key,
        ru_bk_rule_account_rule_name,
        ru_accounting_rule_start_date,
        bk_batch_source_name,
        ar_trx_line_reason_code,
        gl_date,
        ss_code,
        context,
        item_key,
        fiscal_id,
        functional_currency_code,
        gl_posted_dt,
        org_id,
        manual_transaction_flg,
        trx_date,
        trx_number,
        sk_customer_trx_line_id_lint
    FROM {{ source('raw', 'wi_ncr_rev_trx_cur') }}
),

source_n_ar_trx_item_line AS (
    SELECT
        ar_trx_line_key,
        product_key,
        ru_accounting_rule_role,
        ru_unit_of_measure_code,
        ru_unit_std_price_local_amt,
        ru_accounting_rule_start_date,
        ru_debit_quantity,
        ru_debit_unit_amount,
        ru_debit_extended_amount,
        ru_credit_quantity,
        ru_credit_unit_amount,
        ru_credit_extended_amount,
        ru_bk_rule_account_rule_name,
        ar_trx_item_line_cr_dr_type,
        money_only_type,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        revenue_source_cd,
        service_contract_note_txt,
        ru_acctg_rule_duration_mth_cnt
    FROM {{ source('raw', 'n_ar_trx_item_line') }}
),

source_n_ar_trx_line_gl_dist AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        account_class_code,
        account_set_flag,
        ar_trx_key,
        ar_trx_line_key,
        general_ledger_account_key,
        gl_datetime,
        transaction_line_percentage,
        ru_ar_trx_line_func_credit_amt,
        ru_ar_trx_line_trxl_credit_amt,
        ru_ar_trx_line_func_debit_amt,
        ru_ar_trx_line_trxl_debit_amt,
        bk_je_ln_company_code,
        bk_je_ln_journal_entry_num_int,
        bk_je_ln_je_line_number_int,
        bk_je_ln_set_of_books_key,
        bk_fiscal_calendar_code,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        credit_debit_type,
        cogs_pct,
        gl_posting_control_id_int,
        sk_cst_trx_lin_gl_dist_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_ar_trx_line_gl_dist') }}
),

final AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        bk_batch_source_name,
        ar_trx_line_reason_code,
        category_id,
        category_set_id,
        accounting_rule_name,
        accounting_rule_start_date
    FROM source_n_ar_trx_line_gl_dist
)

SELECT * FROM final