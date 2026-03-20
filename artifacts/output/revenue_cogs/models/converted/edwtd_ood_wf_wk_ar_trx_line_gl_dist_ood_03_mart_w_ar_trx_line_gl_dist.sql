{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_ar_trx_line_gl_dist_ood', 'batch', 'edwtd_ood'],
    meta={
        'source_workflow': 'wf_m_WK_AR_TRX_LINE_GL_DIST_OOD',
        'target_table': 'W_AR_TRX_LINE_GL_DIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.872347+00:00'
    }
) }}

WITH 

source_st_ood_fusn_ra_cust_trx_ln_gldt AS (
    SELECT
        account_class,
        account_set_flag,
        acctd_amount,
        amount,
        attribute14,
        attribute2,
        code_combination_id,
        cust_trx_line_gl_dist_id,
        customer_trx_id,
        customer_trx_line_id,
        gl_date,
        org_id,
        line_percent,
        posting_control_id,
        set_of_books_id,
        gl_posted_date,
        creation_date,
        last_update_date,
        split_key,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ood_fusn_ra_cust_trx_ln_gldt') }}
),

source_ex_ood_fusn_ra_cust_trx_ln_gldt AS (
    SELECT
        account_class,
        account_set_flag,
        acctd_amount,
        amount,
        attribute14,
        attribute2,
        code_combination_id,
        cust_trx_line_gl_dist_id,
        customer_trx_id,
        customer_trx_line_id,
        gl_date,
        org_id,
        line_percent,
        posting_control_id,
        set_of_books_id,
        gl_posted_date,
        creation_date,
        last_update_date,
        split_key,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_ood_fusn_ra_cust_trx_ln_gldt') }}
),

source_ex_ood_ra_cust_trx_ln_gldt AS (
    SELECT
        cust_trx_line_gl_dist_id,
        customer_trx_line_id,
        code_combination_id,
        line_percent,
        amount,
        gl_date,
        gl_posted_date,
        account_class,
        customer_trx_id,
        account_set_flag,
        acctd_amount,
        attribute14,
        org_id,
        attribute2,
        posting_control_id,
        creation_date,
        last_update_date,
        create_datetime,
        action_code,
        set_of_books_id,
        exception_type
    FROM {{ source('raw', 'ex_ood_ra_cust_trx_ln_gldt') }}
),

source_n_ar_trx_type AS (
    SELECT
        bk_ar_trx_type_code,
        ar_trx_type_short_code,
        bk_company_code,
        bk_set_of_books_id_int,
        ar_trx_type_description,
        ss_code,
        sk_cust_trx_type_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_ar_trx_type') }}
),

source_sm_st_om_ra_cust_trx_ln_gldt AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        cust_trx_line_gl_dist_id,
        ss_code,
        edw_create_user,
        edw_create_datetime
    FROM {{ source('raw', 'sm_st_om_ra_cust_trx_ln_gldt') }}
),

source_n_fiscal_month AS (
    SELECT
        bk_fiscal_month_number_int,
        bk_fiscal_year_number_int,
        fiscal_month_start_date,
        fiscal_month_end_date,
        fiscal_month_close_date,
        dv_fiscal_month_name,
        bk_fiscal_quarter_number_int,
        bk_fiscal_calendar_code,
        dv_current_fiscal_month_flag,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        dv_fiscal_month_age,
        dv_previous_fscl_month_number,
        dv_fiscal_ytm_flag,
        dv_fiscal_qtm_flag,
        dv_prev_fiscal_year_number,
        dv_previous_fscl_month_flag,
        dv_prev_yr_curr_fscl_mnth_flag,
        dv_number_of_fiscal_week_count
    FROM {{ source('raw', 'n_fiscal_month') }}
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

source_sm_ar_trx AS (
    SELECT
        ar_trx_key,
        sk_customer_trx_id_lint,
        ss_code,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_ar_trx') }}
),

source_sm_ar_trx_line AS (
    SELECT
        ar_trx_line_key,
        sk_customer_trx_line_id_lint,
        ss_code,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_ar_trx_line') }}
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

source_st_ood_ra_cust_trx_ln_gldt AS (
    SELECT
        cust_trx_line_gl_dist_id,
        customer_trx_line_id,
        code_combination_id,
        line_percent,
        amount,
        gl_date,
        gl_posted_date,
        account_class,
        customer_trx_id,
        account_set_flag,
        acctd_amount,
        attribute14,
        org_id,
        attribute2,
        posting_control_id,
        creation_date,
        last_update_date,
        create_datetime,
        set_of_books_id,
        action_code
    FROM {{ source('raw', 'st_ood_ra_cust_trx_ln_gldt') }}
),

transformed_exp_wk_ar_trx_line_gl_dist_ood AS (
    SELECT
    bk_ar_trx_line_gl_distrib_key,
    account_class,
    account_set_flag,
    ar_trx_key,
    ar_trx_line_key,
    general_ledger_account_key,
    gl_datetime,
    transaction_line_percentage,
    ru_ar_trx_line_func_debit_amt,
    ru_ar_trx_line_trxl_debit_amt,
    ru_ar_trx_line_trxl_credit_amt,
    ru_ar_trx_line_func_credit_amt,
    bk_je_ln_company_code,
    bk_je_ln_journal_entry_num_int,
    bk_je_ln_je_line_number_int,
    bk_je_ln_set_of_books_id_int,
    bk_fiscal_calendar_code,
    bk_fiscal_year_number_int,
    bk_fiscal_month_number_int,
    credit_debit_type,
    attribute2,
    sk_cst_trx_lin_gl_dist_id_int,
    source_system_code,
    ar_posting_control,
    gl_posted_dt,
    action_code,
    dml_type
    FROM source_st_ood_ra_cust_trx_ln_gldt
),

final AS (
    SELECT
        bk_ar_trx_line_gl_distrib_key,
        account_class_code,
        account_set_flag,
        ar_trx_key,
        ar_trx_line_key,
        general_ledger_account_key,
        gl_datetime,
        transaction_line_percentage,
        ru_ar_trx_line_func_debit_amt,
        ru_ar_trx_line_trxl_debit_amt,
        ru_ar_trx_line_trxl_credit_amt,
        ru_ar_trx_line_func_credit_amt,
        bk_je_ln_company_code,
        bk_je_ln_journal_entry_num_int,
        bk_je_ln_je_line_number_int,
        bk_je_ln_set_of_books_key,
        bk_fiscal_calendar_code,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        credit_debit_type,
        cogs_pct,
        sk_cst_trx_lin_gl_dist_id_lint,
        ss_code,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        ar_posting_control_key,
        gl_posted_dt,
        action_code,
        dml_type
    FROM transformed_exp_wk_ar_trx_line_gl_dist_ood
)

SELECT * FROM final