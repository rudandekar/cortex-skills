{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_process_activities_dac', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_WF_PROCESS_ACTIVITIES_DAC',
        'target_table': 'ST_CG1_WF_PROCESS_ACTIVITIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.975969+00:00'
    }
) }}

WITH 

source_cg1_wf_process_activities AS (
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
    FROM {{ source('raw', 'cg1_wf_process_activities') }}
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
        source_commit_time,
        global_name
    FROM source_cg1_wf_process_activities
)

SELECT * FROM final