{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_currency_conv_daily_rate_om', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_CURRENCY_CONV_DAILY_RATE_OM',
        'target_table': 'W_CURRENCY_CONV_DAILY_RATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.115749+00:00'
    }
) }}

WITH 

source_st_om_gl_daily_rates AS (
    SELECT
        batch_id,
        from_currency,
        to_currency,
        conversion_date,
        global_name,
        conversion_type,
        conversion_rate,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_gl_daily_rates') }}
),

source_st_mf_gl_daily_rates AS (
    SELECT
        batch_id,
        from_currency,
        to_currency,
        conversion_date,
        global_name,
        conversion_type,
        conversion_rate,
        ges_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_gl_daily_rates') }}
),

transformed_exp_ex_mf_gl_daily_rates AS (
    SELECT
    batch_id,
    from_currency,
    to_currency,
    conversion_date,
    global_name,
    conversion_type,
    conversion_rate,
    create_datetime,
    action_code,
    exception_type
    FROM source_st_mf_gl_daily_rates
),

transformed_exp_w_currency_conv_daily_rate AS (
    SELECT
    batch_id,
    from_currency,
    to_currency,
    conversion_date,
    global_name,
    conversion_type,
    conversion_rate,
    create_datetime,
    action_code
    FROM transformed_exp_ex_mf_gl_daily_rates
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
        edw_update_user,
        action_code
    FROM transformed_exp_w_currency_conv_daily_rate
)

SELECT * FROM final