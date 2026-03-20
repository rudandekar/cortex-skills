{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_opex_headcount_forecast', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_OPEX_HEADCOUNT_FORECAST',
        'target_table': 'W_OPEX_HEADCOUNT_FORECAST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.097085+00:00'
    }
) }}

WITH 

source_w_opex_headcount_forecast AS (
    SELECT
        forecast_fisc_calendar_cd,
        forecast_fisc_year_num,
        forecast_fisc_month_num,
        bk_company_code,
        bk_department_code,
        forecast_sub_fisc_calendar_cd,
        forecast_sub_fisc_year_num,
        forecast_sub_fisc_month_num,
        full_time_employee_head_cnt,
        temporary_employee_head_cnt,
        intern_head_cnt,
        attrition_head_cnt,
        new_hire_head_cnt,
        bk_fiscal_year_number_int,
        bk_fiscal_calendar_cd,
        mnt_3_forcst_subm_wrk_day_num,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_opex_headcount_forecast') }}
),

final AS (
    SELECT
        forecast_fisc_calendar_cd,
        forecast_fisc_year_num,
        forecast_fisc_month_num,
        bk_company_code,
        bk_department_code,
        forecast_sub_fisc_calendar_cd,
        forecast_sub_fisc_year_num,
        forecast_sub_fisc_month_num,
        full_time_employee_head_cnt,
        temporary_employee_head_cnt,
        intern_head_cnt,
        attrition_head_cnt,
        new_hire_head_cnt,
        bk_fiscal_year_number_int,
        bk_fiscal_calendar_cd,
        mnt_3_forcst_subm_wrk_day_num,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_opex_headcount_forecast
)

SELECT * FROM final