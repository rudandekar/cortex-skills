{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccm_ts_mth_qtr_cntl', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_CCM_TS_MTH_QTR_CNTL',
        'target_table': 'WI_CCM_TS_MTH_QTR_CNTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.025182+00:00'
    }
) }}

WITH 

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
        dv_fiscal_mth_sorted_name,
        dv_fiscal_2mth_ago_mth_num_int,
        dv_fiscal_4mth_ago_mth_num_int,
        dv_fiscal_2qtr_ago_mth_num_int,
        dv_fiscal_3qtr_ago_mth_num_int,
        dv_fiscal_quarter_id,
        dv_fiscal_qtr_sorted_name,
        dv_latest_month_in_qtr_flg,
        dv_latest_month_in_year_flg
    FROM {{ source('raw', 'r_fiscal_month_to_year') }}
),

final AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        ccm_fiscal_month,
        month_rank
    FROM source_r_fiscal_month_to_year
)

SELECT * FROM final