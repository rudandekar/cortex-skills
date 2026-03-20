{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_currency_conv_daily_rate', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_CURRENCY_CONV_DAILY_RATE',
        'target_table': 'N_CURRENCY_CONV_DAILY_RATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.856718+00:00'
    }
) }}

WITH 

source_w_currency_conv_daily_rate AS (
    SELECT
        bk_from_currency_code,
        bk_to_currency_code,
        pl_conversion_rate,
        balance_sheet_conv_rate,
        monthly_budget_conv_rate,
        annual_budget_conv_rate,
        sk_from_currency_code,
        sk_to_currency_code,
        sk_conversion_date,
        sk_conversion_type_code,
        bk_conversion_date,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_currency_conv_daily_rate') }}
),

transformed_exp_n_currency_conv_daily_rate AS (
    SELECT
    bk_from_currency_code,
    bk_to_currency_code,
    pl_conversion_rate,
    balance_sheet_conv_rate,
    monthly_budget_conv_rate,
    annual_budget_conv_rate,
    sk_from_currency_code,
    sk_to_currency_code,
    sk_conversion_date,
    sk_conversion_type_code,
    bk_conversion_date,
    edw_create_datetime,
    edw_update_datetime,
    edw_create_user,
    edw_update_user,
    action_code
    FROM source_w_currency_conv_daily_rate
),

final AS (
    SELECT
        bk_from_currency_code,
        bk_to_currency_code,
        pl_conversion_rate,
        balance_sheet_conv_rate,
        monthly_budget_conv_rate,
        annual_budget_conv_rate,
        sk_from_currency_code,
        sk_to_currency_code,
        sk_conversion_date,
        sk_conversion_type_code,
        bk_conversion_date,
        edw_create_datetime,
        edw_update_datetime,
        edw_create_user,
        edw_update_user
    FROM transformed_exp_n_currency_conv_daily_rate
)

SELECT * FROM final