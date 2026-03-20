{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_fin_clarity_proj_expense', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_FIN_CLARITY_PROJ_EXPENSE',
        'target_table': 'ST_FIN_CLARITY_PROJ_EXPENSE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.029861+00:00'
    }
) }}

WITH 

source_ff_fin_clarity_proj_expense AS (
    SELECT
        fiscal_year_month_num_int,
        department_code,
        company_code,
        financial_account_code,
        sub_account_code,
        location_code,
        project_code,
        clarity_project_id,
        product_id,
        fte_count,
        bk_functional_currency_code,
        functional_amt,
        usd_amt,
        platform,
        allocated_flag
    FROM {{ source('raw', 'ff_fin_clarity_proj_expense') }}
),

final AS (
    SELECT
        fiscal_year_month_num_int,
        department_code,
        company_code,
        financial_account_code,
        sub_account_code,
        location_code,
        project_code,
        clarity_project_id,
        product_id,
        fte_count,
        bk_functional_currency_code,
        functional_amt,
        usd_amt,
        platform,
        allocated_flag,
        ges_update_date
    FROM source_ff_fin_clarity_proj_expense
)

SELECT * FROM final