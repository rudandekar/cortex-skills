{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_expense_report_attendee', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_EXPENSE_REPORT_ATTENDEE',
        'target_table': 'N_EXPENSE_REPORT_ATTENDEE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.032548+00:00'
    }
) }}

WITH 

source_w_expense_report_attendee AS (
    SELECT
        bk_expense_rpt_attendee_name,
        attendee_type_cd,
        attendee_title,
        attendee_company_addr,
        attendee_company_name,
        sk_attendee_line_id_int,
        ss_cd,
        expense_report_line_key,
        expense_report_attendee_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_expense_report_attendee') }}
),

final AS (
    SELECT
        bk_expense_rpt_attendee_name,
        attendee_type_cd,
        attendee_title,
        attendee_company_addr,
        attendee_company_name,
        sk_attendee_line_id_int,
        ss_cd,
        expense_report_line_key,
        expense_report_attendee_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_expense_report_attendee
)

SELECT * FROM final