{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ncr_inv_trx_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_WI_NCR_INV_TRX_OOD',
        'target_table': 'WI_NCR_INV_SLS_CRE_FLG_OOD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.804361+00:00'
    }
) }}

WITH 

source_wi_ncr_inv_sls_cre_flg_ood AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_line_key,
        ar_trx_type_short_code,
        context,
        source_type,
        line_id,
        metrix_flag,
        sales_credit_flag
    FROM {{ source('raw', 'wi_ncr_inv_sls_cre_flg_ood') }}
),

source_wi_ncr_inv_val_inv_ood AS (
    SELECT
        ar_trx_line_key,
        gl_date,
        general_ledger_account_key,
        financial_account_code,
        ood_code_combination_id
    FROM {{ source('raw', 'wi_ncr_inv_val_inv_ood') }}
),

source_n_ar_accounting_rule AS (
    SELECT
        bk_accounting_rule_name,
        ar_accounting_rule_description,
        sk_rule_id_int,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_ar_accounting_rule') }}
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

source_n_ar_posting_control AS (
    SELECT
        ar_posting_control_key,
        gl_posted_dt,
        posting_control_dtm,
        sk_gl_posting_control_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_ar_posting_control') }}
),

source_wi_ncr_inv_acct_rule_ood AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_line_key,
        accounting_rule_name,
        accounting_rule_start_date
    FROM {{ source('raw', 'wi_ncr_inv_acct_rule_ood') }}
),

source_wi_ncr_inv_cur_exp_ood AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ss_code,
        gl_date,
        ar_trx_key,
        ru_bk_adjd_ar_trx_key,
        ar_trx_line_key,
        bk_adjd_ar_trx_line_key,
        ar_trx_line_reason_code,
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_type_code,
        sk_customer_trx_line_id_lint,
        sk_customer_trx_id_lint,
        general_ledger_account_key,
        bk_trxl_currency_code,
        trx_date,
        fiscal_id,
        manual_transaction_flg
    FROM {{ source('raw', 'wi_ncr_inv_cur_exp_ood') }}
),

source_wi_ncr_inv_get_lines_ood AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_line_key,
        line_id,
        order_number
    FROM {{ source('raw', 'wi_ncr_inv_get_lines_ood') }}
),

source_wi_ncr_inv_cur_ood AS (
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
        general_ledger_account_key,
        amount,
        acctd_amount,
        fiscal_id,
        org_id,
        manual_transaction_flg
    FROM {{ source('raw', 'wi_ncr_inv_cur_ood') }}
),

source_n_general_ledger_account AS (
    SELECT
        general_ledger_account_key,
        bk_company_code,
        bk_department_code,
        bk_fin_acct_locality_int,
        bk_financial_account_code,
        bk_financial_location_code,
        bk_project_code,
        bk_project_locality_int,
        bk_subaccount_code,
        bk_subaccount_locality_int,
        gl_account_enabled_flag,
        gl_account_end_date,
        gl_account_start_date,
        gl_account_type_code,
        set_of_books_key,
        sk_code_combination_id_int,
        ss_code,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user
    FROM {{ source('raw', 'n_general_ledger_account') }}
),

source_n_ar_trx AS (
    SELECT
        ar_trx_key,
        bk_ar_trx_number,
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_type_code,
        ar_trx_class_type,
        ar_trx_datetime,
        ar_trx_reason_code,
        ar_trx_complete_flag,
        bk_trxl_currency_code,
        ru_cust_purchase_order_number,
        ru_ar_cmdm_adjustment_type,
        ship_to_customer_key,
        bill_to_customer_key,
        sold_to_customer_key,
        ru_bk_adjd_ar_trx_key,
        sk_customer_trx_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_ar_trx') }}
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

source_ex_ncr_inv_cur_exp_ood AS (
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
    FROM {{ source('raw', 'ex_ncr_inv_cur_exp_ood') }}
),

source_n_source_system_codes AS (
    SELECT
        source_system_code,
        source_system_name,
        database_name,
        company,
        edw_create_date,
        edw_create_user,
        edw_update_date,
        edw_update_user,
        global_name,
        gmt_offset
    FROM {{ source('raw', 'n_source_system_codes') }}
),

source_n_ar_trx_line AS (
    SELECT
        ar_trx_line_key,
        ar_trx_key,
        bk_adjd_ar_trx_line_key,
        bk_company_code,
        bk_set_of_books_key,
        bk_ar_trx_number,
        bk_ar_trx_type_code,
        bk_ar_trx_line_type,
        bk_ar_trx_line_number_int,
        ar_trx_line_reason_code,
        bk_saf_set_of_books_key,
        bk_saf_company_code,
        bk_saf_id_int,
        applied_transaction_type,
        ar_trx_adjustment_line_role,
        ar_trx_line_creation_datetime,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_ar_trx_line') }}
),

final AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        ar_trx_line_key,
        ar_trx_type_short_code,
        context,
        source_type,
        line_id,
        metrix_flag,
        sales_credit_flag
    FROM source_n_ar_trx_line
)

SELECT * FROM final