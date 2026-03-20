{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_process_activities', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_WF_PROCESS_ACTIVITIES',
        'target_table': 'STG_CG1_WF_PROCESS_ACTIVITIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.701056+00:00'
    }
) }}

WITH 

source_stg_cg1_wf_process_activities AS (
    SELECT
        activity_item_type,
        activity_name,
        custom_level,
        default_result,
        icon_geometry,
        instance_id,
        instance_label,
        perform_role,
        perform_role_type,
        process_item_type,
        process_name,
        process_version,
        protect_level,
        security_group_id,
        start_end,
        user_comment,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_wf_process_activities') }}
),

source_cg1_wf_process_activities AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        process_item_type,
        process_name,
        process_version,
        activity_item_type,
        activity_name,
        instance_id,
        instance_label,
        perform_role_type,
        protect_level,
        custom_level,
        start_end,
        default_result,
        icon_geometry,
        perform_role,
        user_comment,
        security_group_id
    FROM {{ source('raw', 'cg1_wf_process_activities') }}
),

transformed_exp_cg1_wf_process_activities AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    process_item_type,
    process_name,
    process_version,
    activity_item_type,
    activity_name,
    instance_id,
    instance_label,
    perform_role_type,
    protect_level,
    custom_level,
    start_end,
    default_result,
    icon_geometry,
    perform_role,
    user_comment,
    security_group_id
    FROM source_cg1_wf_process_activities
),

final AS (
    SELECT
        activity_item_type,
        activity_name,
        custom_level,
        default_result,
        icon_geometry,
        instance_id,
        instance_label,
        perform_role,
        perform_role_type,
        process_item_type,
        process_name,
        process_version,
        protect_level,
        security_group_id,
        start_end,
        user_comment,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_wf_process_activities
)

SELECT * FROM final