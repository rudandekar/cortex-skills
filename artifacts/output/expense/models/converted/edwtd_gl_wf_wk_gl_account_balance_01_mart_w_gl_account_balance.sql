{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_gl_account_balance', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_GL_ACCOUNT_BALANCE',
        'target_table': 'W_GL_ACCOUNT_BALANCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.852006+00:00'
    }
) }}

WITH 

source_st_mf_gl_balances AS (
    SELECT
        batch_id,
        actual_flag,
        begin_balance_cr,
        begin_balance_dr,
        code_combination_id,
        currency_code,
        ges_pk_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        period_name,
        period_net_cr,
        period_net_dr,
        period_num,
        period_type,
        period_year,
        revaluation_status,
        set_of_books_id,
        translated_flag,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_gl_balances') }}
),

transformed_exp_ex_w_gl_account_balance AS (
    SELECT
    batch_id,
    actual_flag,
    begin_balance_cr,
    begin_balance_dr,
    code_combination_id,
    currency_code,
    ges_pk_id,
    ges_update_date,
    global_name,
    last_updated_by,
    last_update_date,
    period_name,
    period_net_cr,
    period_net_dr,
    period_num,
    period_type,
    period_year,
    revaluation_status,
    set_of_books_id,
    translated_flag,
    create_datetime,
    action_code,
    exception_type
    FROM source_st_mf_gl_balances
),

transformed_exp_w_gl_account_balance AS (
    SELECT
    gl_account_balance_key,
    bk_fiscal_calendar_code,
    bk_fiscal_month_number_int,
    bk_fiscal_year_number_int,
    bk_posting_dt,
    bk_stated_currency_cd,
    general_ledger_account_key,
    credit_balance_amt,
    debit_balance_amt,
    dv_fiscal_year_month_num_int,
    period_activity_debit_amt,
    period_activity_credit_amt,
    sk_ges_pk_id_int,
    ss_code,
    action_code,
    'I' AS dml_type
    FROM transformed_exp_ex_w_gl_account_balance
),

final AS (
    SELECT
        gl_account_balance_key,
        bk_fiscal_calendar_cd,
        bk_fiscal_month_number_int,
        bk_fiscal_year_number_int,
        bk_posting_dt,
        bk_stated_currency_cd,
        general_ledger_account_key,
        credit_balance_amt,
        debit_balance_amt,
        dv_fiscal_year_month_num_int,
        period_activity_debit_amt,
        period_activity_credit_amt,
        sk_ges_pk_id_int,
        ss_cd,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type
    FROM transformed_exp_w_gl_account_balance
)

SELECT * FROM final