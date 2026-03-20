{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_expense_future_budget', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_EXPENSE_FUTURE_BUDGET',
        'target_table': 'N_EXPENSE_FUTURE_BUDGET',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.067816+00:00'
    }
) }}

WITH 

source_w_expense_future_budget AS (
    SELECT
        fiscal_month_number_int,
        fiscal_year_number_int,
        fiscal_calendar_cd,
        expense_future_publication_dt,
        company_cd,
        department_cd,
        financial_account_cd,
        financial_account_locality_int,
        functional_budget_amt,
        usd_budget_amt,
        usd_budget_at_actual_rate_amt,
        usd_budget_at_commit_rate_amt,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type,
        functional_currency_cd
    FROM {{ source('raw', 'w_expense_future_budget') }}
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
        functional_budget_amt,
        usd_budget_amt,
        usd_budget_at_actual_rate_amt,
        usd_budget_at_commit_rate_amt,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        functional_currency_cd
    FROM source_w_expense_future_budget
)

SELECT * FROM final