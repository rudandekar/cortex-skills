{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_webex_meet_acts_f', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_WEBEX_MEET_ACTS_F',
        'target_table': 'EL_WEBEX_MEET_ACTS_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.493431+00:00'
    }
) }}

WITH 

source_st_webex_meet_acts AS (
    SELECT
        session_id,
        site_id,
        row_id,
        service_req_num,
        session_key,
        user_id,
        user_name,
        participant_email,
        participant_type,
        participant_start_dtm,
        participant_end_dtm,
        participant_duration_minutes,
        session_name,
        session_start_dtm,
        session_end_dtm,
        host_timezone
    FROM {{ source('raw', 'st_webex_meet_acts') }}
),

final AS (
    SELECT
        session_id,
        site_id,
        row_id,
        service_req_num,
        session_key,
        user_id,
        user_name,
        participant_email,
        participant_type,
        participant_start_dtm,
        participant_end_dtm,
        participant_duration_minutes,
        session_name,
        session_start_dtm,
        session_end_dtm,
        host_timezone,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_webex_meet_acts
)

SELECT * FROM final