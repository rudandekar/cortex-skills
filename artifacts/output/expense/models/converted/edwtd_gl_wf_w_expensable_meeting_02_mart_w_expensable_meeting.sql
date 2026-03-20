{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_expensable_meeting', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_EXPENSABLE_MEETING',
        'target_table': 'W_EXPENSABLE_MEETING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.835527+00:00'
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
        bk_expensable_meeting_id_int,
        creation_dtm,
        dv_creation_dt,
        meeting_start_dt,
        meeting_end_dt,
        meeting_status_cd,
        expected_attendees_cnt,
        estimated_meeting_budget_desc,
        meeting_type_cd,
        cisco_worker_party_key,
        meeting_site_cd,
        financial_account_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_expensable_meeting
)

SELECT * FROM final