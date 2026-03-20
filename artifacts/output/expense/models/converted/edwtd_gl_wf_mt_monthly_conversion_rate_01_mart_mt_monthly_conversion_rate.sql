{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_monthly_conversion_rate', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_MT_MONTHLY_CONVERSION_RATE',
        'target_table': 'MT_MONTHLY_CONVERSION_RATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.725700+00:00'
    }
) }}

WITH 

source_n_currency_conv_daily_rate AS (
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
    FROM {{ source('raw', 'n_currency_conv_daily_rate') }}
),

source_n_fiscal_month AS (
    SELECT
        bk_fiscal_month_number_int,
        bk_fiscal_year_number_int,
        fiscal_month_start_date,
        fiscal_month_end_date,
        fiscal_month_close_date,
        dv_fiscal_month_name,
        bk_fiscal_quarter_number_int,
        bk_fiscal_calendar_code,
        dv_current_fiscal_month_flag,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        dv_fiscal_month_age,
        dv_previous_fscl_month_number,
        dv_fiscal_ytm_flag,
        dv_fiscal_qtm_flag,
        dv_prev_fiscal_year_number,
        dv_previous_fscl_month_flag,
        dv_prev_yr_curr_fscl_mnth_flag,
        dv_number_of_fiscal_week_count
    FROM {{ source('raw', 'n_fiscal_month') }}
),

final AS (
    SELECT
        bk_from_currency_cd,
        bk_to_currency_cd,
        pl_conversion_rt,
        balance_sheet_conv_rt,
        monthly_budget_conv_rt,
        annual_budget_conv_rt,
        fiscal_year_month_num_int,
        edw_create_dtm,
        edw_update_dtm,
        edw_create_user,
        edw_update_user
    FROM source_n_fiscal_month
)

SELECT * FROM final