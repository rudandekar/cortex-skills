{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_expensable_meeting', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_EXPENSABLE_MEETING',
        'target_table': 'ST_EXPENSABLE_MEETING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.616680+00:00'
    }
) }}

WITH 

source_ff_expensable_meeting AS (
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
        meeting_site_cd
    FROM {{ source('raw', 'ff_expensable_meeting') }}
),

transformed_exp_st_expensable_meeting AS (
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
    'I' AS action_code
    FROM source_ff_expensable_meeting
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
        action_code
    FROM transformed_exp_st_expensable_meeting
)

SELECT * FROM final