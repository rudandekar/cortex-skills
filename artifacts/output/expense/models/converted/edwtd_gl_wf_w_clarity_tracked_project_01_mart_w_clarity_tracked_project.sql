{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_clarity_tracked_project', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_W_CLARITY_TRACKED_PROJECT',
        'target_table': 'W_CLARITY_TRACKED_PROJECT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.988446+00:00'
    }
) }}

WITH 

source_w_clarity_tracked_project AS (
    SELECT
        bk_clarity_project_type_cd,
        bk_clarity_project_num,
        bk_ss_clarity_tracked_proj_cd,
        project_name,
        project_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_clarity_tracked_project') }}
),

final AS (
    SELECT
        bk_clarity_project_type_cd,
        bk_clarity_project_num,
        bk_ss_clarity_tracked_proj_cd,
        project_name,
        project_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_clarity_tracked_project
)

SELECT * FROM final