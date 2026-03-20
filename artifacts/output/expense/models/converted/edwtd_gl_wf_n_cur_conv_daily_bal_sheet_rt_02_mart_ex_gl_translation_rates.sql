{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cur_conv_daily_bal_sheet_rt', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_CUR_CONV_DAILY_BAL_SHEET_RT',
        'target_table': 'EX_GL_TRANSLATION_RATES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.593572+00:00'
    }
) }}

WITH 

source_st_mf_gl_translation_rates AS (
    SELECT
        batch_id,
        actual_flag,
        avg_rate,
        created_by,
        creation_date,
        eop_rate,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        period_name,
        set_of_books_id,
        to_currency_code,
        update_flag,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_gl_translation_rates') }}
),

source_wi_currency_conv_bal_rate AS (
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
    FROM {{ source('raw', 'wi_currency_conv_bal_rate') }}
),

transformed_exp_m_n_cur_conv_daily_rt_exp_future1 AS (
    SELECT
    bk_from_currency_code,
    bk_to_currency_code,
    pl_conversion_rate,
    eop_rate,
    bk_conversion_date,
    sk_conversion_type_code,
    edw_create_datetime,
    edw_create_user,
    action_code
    FROM source_wi_currency_conv_bal_rate
),

final AS (
    SELECT
        batch_id,
        actual_flag,
        avg_rate,
        created_by,
        creation_date,
        eop_rate,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        period_name,
        set_of_books_id,
        to_currency_code,
        update_flag,
        create_datetime,
        action_code,
        exception_type
    FROM transformed_exp_m_n_cur_conv_daily_rt_exp_future1
)

SELECT * FROM final