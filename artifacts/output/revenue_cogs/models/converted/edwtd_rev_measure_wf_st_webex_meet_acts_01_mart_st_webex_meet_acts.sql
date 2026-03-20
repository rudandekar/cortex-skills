{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_webex_meet_acts', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_WEBEX_MEET_ACTS',
        'target_table': 'ST_WEBEX_MEET_ACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.343427+00:00'
    }
) }}

WITH 

source_ff_bv_webex_session_participant_acts AS (
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
    FROM {{ source('raw', 'ff_bv_webex_session_participant_acts') }}
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
        host_timezone
    FROM source_ff_bv_webex_session_participant_acts
)

SELECT * FROM final