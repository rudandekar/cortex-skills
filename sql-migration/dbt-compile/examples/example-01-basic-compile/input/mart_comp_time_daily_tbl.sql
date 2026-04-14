{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_comptime_current_pay_period', 'TODO_freq', 'TODO_domain'],
    meta={
        'source_workflow': 'wf_m_COMPTIME_Current_Pay_Period',
        'target_table': 'COMP_TIME_DAILY_TBL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-15T13:12:35.100724+00:00'
    }
) }}

WITH 

source_u0287d01 AS (
    SELECT
        ssn,
        name,
        current_acct,
        current_org,
        flsa_status,
        comp_time_cur_bal,
        comp_time_year_earned,
        pp_end_date,
        daily_date_earned,
        comp_time_rate
        /* Note: 2 additional columns omitted - review source schema */
    FROM {{ source('raw', 'u0287d01') }}
),

source_pay_period AS (
    SELECT
        pp_num,
        pp_end_year,
        pp_start_dte,
        pp_end_dte,
        lv_num,
        lv_year,
        pay_dte,
        curr_pp_flag,
        holiday_1,
        holiday_2
    FROM {{ source('raw', 'pay_period') }}
),

transformed_exp_build_pay_period AS (
    SELECT
    pp_num,
    pp_end_year,
    TO_CHAR(PP_END_YEAR) || v_PP_NUM AS o_pay_period,
    SETVARIABLE($$MAP_PP_YEAR_NUM, v_PAY_PERIOD) AS o_map_pp_year_num,
    SETVARIABLE($$MAP_PP_END_YEAR, PP_END_YEAR) AS o_pp_end_year,
    SETVARIABLE($$MAP_PP_NUM, PP_NUM) AS o_pp_num
    FROM source_pay_period
),

transformed_exp_final AS (
    SELECT
    pay_period,
    map_pp_year_num,
    pp_end_year,
    pp_num
    FROM transformed_exp_build_pay_period
),

final AS (
    SELECT
        pp_end_year,
        pp_num,
        pp_year_num,
        ssn,
        name,
        current_acct,
        current_org,
        flsa_status,
        comp_time_cur_bal,
        comp_time_year_earned,
        pp_end_date,
        daily_date_earned,
        comp_time_rate,
        comp_time_hours,
        comp_time_undef
    FROM transformed_exp_final
)

SELECT * FROM final
