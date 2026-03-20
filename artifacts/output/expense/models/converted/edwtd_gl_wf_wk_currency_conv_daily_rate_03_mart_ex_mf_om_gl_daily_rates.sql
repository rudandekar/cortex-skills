{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_currency_conv_daily_rate_mf', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_CURRENCY_CONV_DAILY_RATE_MF',
        'target_table': 'EX_MF_OM_GL_DAILY_RATES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.941914+00:00'
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
    action_code,
    'E_FINLGL_BATCH' AS edw_create_user,
    CURRENT_TIMESTAMP() AS edw_create_datetime,
    'E_FINLGL_BATCH' AS edw_update_user,
    CURRENT_TIMESTAMP() AS edw_update_datetime
    FROM source_st_mf_gl_daily_rates
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
    FROM transformed_exp_w_currency_conv_daily_rate
),

final AS (
    SELECT
        batch_id,
        from_currency,
        to_currency,
        conversion_date,
        conversion_rate,
        conversion_type,
        global_name,
        create_datetime,
        action_code,
        exception_type
    FROM transformed_exp_ex_mf_gl_daily_rates
)

SELECT * FROM final