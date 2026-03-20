{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_process_activities', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_WF_PROCESS_ACTIVITIES',
        'target_table': 'STG_CSF_WF_PROCESS_ACTIVITIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.853595+00:00'
    }
) }}

WITH 

source_stg_csf_wf_process_activities AS (
    SELECT
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
        security_group_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_wf_process_activities') }}
),

source_csf_wf_process_activities AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
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
        security_group_id,
        zd_edition_name,
        zd_sync
    FROM {{ source('raw', 'csf_wf_process_activities') }}
),

transformed_exp_csf_wf_process_activities AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
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
    security_group_id,
    zd_edition_name,
    zd_sync
    FROM source_csf_wf_process_activities
),

final AS (
    SELECT
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
        security_group_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_wf_process_activities
)

SELECT * FROM final