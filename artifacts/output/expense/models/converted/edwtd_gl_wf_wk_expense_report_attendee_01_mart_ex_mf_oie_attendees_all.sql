{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_expense_report_attendee', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_EXPENSE_REPORT_ATTENDEE',
        'target_table': 'EX_MF_OIE_ATTENDEES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.124764+00:00'
    }
) }}

WITH 

source_sm_expense_report_attendee AS (
    SELECT
        expense_report_attendee_key,
        sk_attendee_line_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_expense_report_attendee') }}
),

source_sm_expense_report_line AS (
    SELECT
        expense_report_line_key,
        sk_report_line_id_int,
        bk_ss_cd,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_expense_report_line') }}
),

source_st_mf_oie_attendees_all AS (
    SELECT
        batch_id,
        attendee_line_id,
        report_line_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        employee_flag,
        employee_id,
        attendee_type,
        name,
        title_cd,
        employer,
        employer_address,
        tax_id,
        org_id,
        last_update_login,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_oie_attendees_all') }}
),

transformed_exp_expense_report_attendee AS (
    SELECT
    expense_report_attendee_key,
    name,
    expense_report_line_key,
    attendee_type,
    title_cd,
    employer_address,
    employer,
    attendee_line_id,
    ss_cd
    FROM source_st_mf_oie_attendees_all
),

transformed_exp_ex_expense_report_attendee AS (
    SELECT
    batch_id,
    attendee_line_id,
    report_line_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    employee_flag,
    employee_id,
    attendee_type,
    name,
    title_cd,
    employer,
    employer_address,
    tax_id,
    org_id,
    last_update_login,
    ges_update_date,
    global_name,
    create_datetime,
    action_code,
    'RI' AS exception_type
    FROM transformed_exp_expense_report_attendee
),

final AS (
    SELECT
        batch_id,
        attendee_line_id,
        report_line_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        employee_flag,
        employee_id,
        attendee_type,
        name,
        title_cd,
        employer,
        employer_address,
        tax_id,
        org_id,
        last_update_login,
        ges_update_date,
        global_name,
        create_datetime,
        action_code,
        exception_type
    FROM transformed_exp_ex_expense_report_attendee
)

SELECT * FROM final