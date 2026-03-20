{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_expense_future_commit', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_EXPENSE_FUTURE_COMMIT',
        'target_table': 'W_EXPENSE_FUTURE_COMMIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.625130+00:00'
    }
) }}

WITH 

source_st_pl_expn_com_publish AS (
    SELECT
        batch_id,
        dept_id,
        account_id,
        m1,
        m2,
        m3,
        m4,
        m5,
        m6,
        m7,
        m8,
        m9,
        m10,
        m11,
        m12,
        refresh_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_pl_expn_com_publish') }}
),

transformed_exp_expense_future_commit AS (
    SELECT
    bk_fiscal_year_number_int,
    bk_fiscal_month_number_int,
    expense_future_publication_dt,
    fiscal_calendar_cd,
    company_cd,
    department_cd,
    financial_account_cd,
    financial_account_locality_int,
    usd_commit_at_actual_rate_amt,
    functional_commit_amt,
    usd_commit_amt,
    functional_currency_cd,
    dv_fiscal_year_month_int,
    'I' AS action_code,
    'U' AS dml_type
    FROM source_st_pl_expn_com_publish
),

final AS (
    SELECT
        fiscal_month_number_int,
        fiscal_year_number_int,
        fiscal_calendar_cd,
        expense_future_publication_dt,
        company_cd,
        department_cd,
        financial_account_cd,
        financial_account_locality_int,
        functional_commit_amt,
        usd_commit_amt,
        usd_commit_at_actual_rate_amt,
        usd_commit_at_budget_rate_amt,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type,
        functional_currency_cd
    FROM transformed_exp_expense_future_commit
)

SELECT * FROM final