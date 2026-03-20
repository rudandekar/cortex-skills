{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_lab_recreates_acts', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_LAB_RECREATES_ACTS',
        'target_table': 'ST_LAB_RECREATES_ACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.992503+00:00'
    }
) }}

WITH 

source_ff_cx_labrecreate_acts AS (
    SELECT
        lab_recreate_id,
        service_req_num,
        update_originator,
        updater_type,
        update_duration_secs,
        case_originator,
        case_create_time,
        case_update_time,
        posted_tool_name,
        posted_via,
        priority,
        status,
        tce
    FROM {{ source('raw', 'ff_cx_labrecreate_acts') }}
),

final AS (
    SELECT
        lab_recreate_id,
        service_req_num,
        update_originator,
        updater_type,
        update_duration_secs,
        case_originator,
        case_create_time,
        case_update_time,
        posted_tool_name,
        posted_via,
        priority,
        status,
        tce
    FROM source_ff_cx_labrecreate_acts
)

SELECT * FROM final