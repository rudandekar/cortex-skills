{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ mf_oie_attendees_all', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_ MF_OIE_ATTENDEES_ALL',
        'target_table': 'ST_MF_OIE_ATTENDEES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.991887+00:00'
    }
) }}

WITH 

source_ff_mf_oie_attendees_all AS (
    SELECT
        batch_id,
        create_datetime,
        action_code,
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
        title,
        employer,
        employer_address,
        tax_id,
        org_id,
        last_update_login,
        ges_update_date,
        global_name
    FROM {{ source('raw', 'ff_mf_oie_attendees_all') }}
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
        action_code
    FROM source_ff_mf_oie_attendees_all
)

SELECT * FROM final