{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_general_ledger', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_GENERAL_LEDGER',
        'target_table': 'EX_ED_COMPANY_SOURCE_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.096302+00:00'
    }
) }}

WITH 

source_st_mf_gl_code_combinations AS (
    SELECT
        batch_id,
        code_combination_id,
        global_name,
        account_type,
        chart_of_accounts_id,
        enabled_flag,
        preserve_flag,
        segment1,
        segment2,
        segment3,
        segment4,
        segment5,
        segment6,
        start_date_active,
        end_date_active,
        ges_update_date,
        create_datetime,
        action_code,
        last_update_date
    FROM {{ source('raw', 'st_mf_gl_code_combinations') }}
),

source_st_ed_company_source_map AS (
    SELECT
        batch_id,
        company,
        global_name,
        set_of_books_id,
        currency_code,
        start_fiscal_period_id,
        end_fiscal_period_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ed_company_source_map') }}
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

transformed_exp_st_mf_gl_code_combinations_work AS (
    SELECT
    gl_type_code,
    bk_company_code,
    start_tv_date,
    end_tv_date,
    set_of_books_key,
    bk_functional_currency_code,
    action_code,
    rank_index,
    dml_type
    FROM source_n_fiscal_month
),

transformed_exp_w_general_ledger AS (
    SELECT
    gl_type_code,
    bk_company_code,
    start_tv_date,
    end_tv_date,
    set_of_books_key,
    bk_functional_currency_code,
    action_code
    FROM transformed_exp_st_mf_gl_code_combinations_work
),

transformed_exp_st_ed_company_source_map_ri AS (
    SELECT
    batch_id,
    company,
    global_name,
    set_of_books_id,
    currency_code,
    start_fiscal_period_id,
    end_fiscal_period_id,
    create_datetime,
    action_code,
    'RI' AS exception_type
    FROM transformed_exp_w_general_ledger
),

final AS (
    SELECT
        batch_id,
        company,
        global_name,
        set_of_books_id,
        currency_code,
        start_fiscal_period_id,
        end_fiscal_period_id,
        create_datetime,
        action_code,
        exception_type
    FROM transformed_exp_st_ed_company_source_map_ri
)

SELECT * FROM final