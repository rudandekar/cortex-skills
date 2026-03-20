{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_opex_expense_forecast', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_OPEX_EXPENSE_FORECAST',
        'target_table': 'FF_OPEX_EXPENSE_FORECAST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.865362+00:00'
    }
) }}

WITH 

source_expense_forecast_opx AS (
    SELECT
        processed_fiscal_month,
        fiscal_month,
        dept_id,
        account_id,
        amount,
        active_flag,
        refresh_date,
        scenario
    FROM {{ source('raw', 'expense_forecast_opx') }}
),

final AS (
    SELECT
        processed_fiscal_month,
        fiscal_month,
        dept_id,
        account_id,
        amount,
        active_flag,
        refresh_date,
        scenario
    FROM source_expense_forecast_opx
)

SELECT * FROM final