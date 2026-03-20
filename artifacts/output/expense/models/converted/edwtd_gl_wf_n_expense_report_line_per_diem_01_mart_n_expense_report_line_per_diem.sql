{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_expense_report_line_per_diem', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_EXPENSE_REPORT_LINE_PER_DIEM',
        'target_table': 'N_EXPENSE_REPORT_LINE_PER_DIEM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.073467+00:00'
    }
) }}

WITH 

source_w_expense_report_line_per_diem AS (
    SELECT
        end_multiday_expns_dtm,
        first_day_free_accomod_cnt,
        first_day_free_brkfast_cnt,
        first_day_free_dinner_cnt,
        first_day_free_lunch_cnt,
        first_day_per_diem_rt,
        first_day_deduction_add_amt,
        interim_days_free_accom_cnt,
        interim_days_free_brkfast_cnt,
        interim_days_free_dinner_cnt,
        interim_days_free_lunch_cnt,
        interim_days_per_diem_rt,
        interim_days_deduction_add_amt,
        last_day_free_accom_cnt,
        last_day_free_brkfast_cnt,
        last_day_free_dinner_cnt,
        last_day_free_lunch_cnt,
        last_day_per_diem_rt,
        last_day_deduction_add_amt,
        expense_report_line_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_expense_report_line_per_diem') }}
),

final AS (
    SELECT
        end_multiday_expns_dtm,
        first_day_free_accomod_cnt,
        first_day_free_brkfast_cnt,
        first_day_free_dinner_cnt,
        first_day_free_lunch_cnt,
        first_day_per_diem_rt,
        first_day_deduction_add_amt,
        interim_days_free_accom_cnt,
        interim_days_free_brkfast_cnt,
        interim_days_free_dinner_cnt,
        interim_days_free_lunch_cnt,
        interim_days_per_diem_rt,
        interim_days_deduction_add_amt,
        last_day_free_accom_cnt,
        last_day_free_brkfast_cnt,
        last_day_free_dinner_cnt,
        last_day_free_lunch_cnt,
        last_day_per_diem_rt,
        last_day_deduction_add_amt,
        expense_report_line_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_expense_report_line_per_diem
)

SELECT * FROM final