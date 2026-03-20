{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_pa_obj_status_changes', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_OBJ_STATUS_CHANGES',
        'target_table': 'CSF_PA_OBJ_STATUS_CHANGES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.916745+00:00'
    }
) }}

WITH 

source_stg_csf_pa_obj_status_changes AS (
    SELECT
        obj_status_change_id,
        object_type,
        object_id,
        status_type,
        new_project_status_code,
        new_project_system_status_code,
        old_project_status_code,
        old_project_system_status_code,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        change_comment,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_obj_status_changes') }}
),

source_csf_pa_obj_status_changes AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        obj_status_change_id,
        object_type,
        object_id,
        status_type,
        new_project_status_code,
        new_project_system_status_code,
        old_project_status_code,
        old_project_system_status_code,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        change_comment
    FROM {{ source('raw', 'csf_pa_obj_status_changes') }}
),

transformed_exp_csf_pa_obj_status_changes AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    obj_status_change_id,
    object_type,
    object_id,
    status_type,
    new_project_status_code,
    new_project_system_status_code,
    old_project_status_code,
    old_project_system_status_code,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    change_comment
    FROM source_csf_pa_obj_status_changes
),

final AS (
    SELECT
        obj_status_change_id,
        object_type,
        object_id,
        status_type,
        new_project_status_code,
        new_project_system_status_code,
        old_project_status_code,
        old_project_system_status_code,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        change_comment,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_obj_status_changes
)

SELECT * FROM final