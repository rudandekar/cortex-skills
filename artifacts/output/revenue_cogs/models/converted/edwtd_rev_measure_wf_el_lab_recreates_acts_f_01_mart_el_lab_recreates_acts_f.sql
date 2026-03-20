{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_lab_recreates_acts_f', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_LAB_RECREATES_ACTS_F',
        'target_table': 'EL_LAB_RECREATES_ACTS_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.351437+00:00'
    }
) }}

WITH 

source_st_lab_recreates_acts AS (
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
    FROM {{ source('raw', 'st_lab_recreates_acts') }}
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
        tce,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_lab_recreates_acts
)

SELECT * FROM final