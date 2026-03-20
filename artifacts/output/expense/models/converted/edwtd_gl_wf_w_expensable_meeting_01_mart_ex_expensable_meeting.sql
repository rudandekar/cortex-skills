{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_expensable_meeting', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_EXPENSABLE_MEETING',
        'target_table': 'EX_EXPENSABLE_MEETING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.067095+00:00'
    }
) }}

WITH 

source_st_expensable_meeting AS (
    SELECT
        client_meeting_id,
        creation_date,
        meeting_start_dt,
        meeting_end_dt,
        meeting_status,
        expected_attendees_cnt,
        estimated_meeting_budget,
        event_type_attr,
        meeting_gl,
        requestor_email,
        meeting_site_cd,
        action_code
    FROM {{ source('raw', 'st_expensable_meeting') }}
),

final AS (
    SELECT
        client_meeting_id,
        creation_date,
        meeting_start_dt,
        meeting_end_dt,
        meeting_status,
        expected_attendees_cnt,
        estimated_meeting_budget,
        event_type_attr,
        meeting_gl,
        requestor_email,
        meeting_site_cd,
        edw_create_datetime,
        action_code,
        exception_type
    FROM source_st_expensable_meeting
)

SELECT * FROM final