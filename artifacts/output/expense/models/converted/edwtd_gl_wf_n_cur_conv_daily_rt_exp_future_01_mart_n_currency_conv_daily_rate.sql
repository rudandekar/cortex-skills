{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cur_conv_daily_rt_exp_future', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_CUR_CONV_DAILY_RT_EXP_FUTURE',
        'target_table': 'N_CURRENCY_CONV_DAILY_RATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.867887+00:00'
    }
) }}

WITH 

source_st_pl_fx_rate_publish AS (
    SELECT
        batch_id,
        fiscal_month_id,
        currency_code,
        conversion_rate,
        refresh_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_pl_fx_rate_publish') }}
),

transformed_exp_m_n_cur_conv_daily_rt_exp_future1 AS (
    SELECT
    from_currency_code,
    to_currency_code,
    fiscal_month_start_date,
    pl_conversion_rate,
    dv_conversion_rate,
    fiscal_year_month_num_int,
    sk_conversion_type_code
    FROM source_st_pl_fx_rate_publish
),

transformed_exp_m_n_cur_conv_daily_rt_exp_future AS (
    SELECT
    from_currency_code,
    to_currency_code,
    fiscal_month_start_date,
    pl_conversion_rate,
    dv_conversion_rate,
    fiscal_year_month_num_int,
    sk_conversion_type_code
    FROM transformed_exp_m_n_cur_conv_daily_rt_exp_future1
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
    FROM transformed_exp_m_n_cur_conv_daily_rt_exp_future
)

SELECT * FROM final