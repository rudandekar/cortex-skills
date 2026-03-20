{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_rstd_rev_aggr_ref_incr', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_RSTD_REV_AGGR_REF_INCR',
        'target_table': 'WI_GL_CHNL_MSR_PRDT_MOTH_CNTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.176757+00:00'
    }
) }}

WITH 

source_wi_gl_chnl_msr_prdt_moth_cntl AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        run_batch_id
    FROM {{ source('raw', 'wi_gl_chnl_msr_prdt_moth_cntl') }}
),

source_wi_gl_channel_msr_month_cntl AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        run_batch_id,
        inc_flag
    FROM {{ source('raw', 'wi_gl_channel_msr_month_cntl') }}
),

source_r_fiscal_month_to_year AS (
    SELECT
        fiscal_year_month_int,
        fiscal_calendar_code,
        fiscal_year_number_int,
        fiscal_month_number_int,
        current_fiscal_month_flag,
        fiscal_month_age,
        fiscal_month_close_date,
        fiscal_month_end_date,
        fiscal_month_name,
        fiscal_month_start_date,
        fiscal_qtm_flag,
        fiscal_ytm_flag,
        number_of_fscl_week_month_cnt,
        prev_yr_curr_fscl_mnth_flag,
        previous_fscl_month_flag,
        previous_fscl_month_number,
        fiscal_quarter_number_int,
        current_fiscal_qtr_flag,
        fiscal_qtr_age,
        fiscal_quarter_end_date,
        fiscal_quarter_name,
        fiscal_quarter_start_date,
        fiscal_ytq_flag,
        number_of_fiscal_week_qtr_cnt,
        prev_yr_curr_fscl_qtr_flag,
        previous_fiscal_qtr_flag,
        previous_fiscal_qtr_number,
        current_fiscal_year_flag,
        fiscal_year_age,
        fiscal_year_end_date,
        fiscal_year_start_date,
        number_of_fiscal_week_year_cnt,
        previous_fiscal_year_flag,
        previous_fiscal_year_number,
        fiscal_month_ago_month_number,
        fiscal_year_ago_month_number,
        fiscal_qtr_ago_month_number,
        fiscal_year_quarter_number_int,
        fiscal_2year_ago_month_num_int,
        fiscal_3year_ago_month_num_int,
        dv_fiscal_mth_sorted_name
    FROM {{ source('raw', 'r_fiscal_month_to_year') }}
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        fiscal_year_month_int,
        run_batch_id
    FROM source_r_fiscal_month_to_year
)

SELECT * FROM final