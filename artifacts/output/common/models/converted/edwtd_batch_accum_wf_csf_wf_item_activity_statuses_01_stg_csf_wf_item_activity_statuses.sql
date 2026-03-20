{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_item_activity_statuses', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_WF_ITEM_ACTIVITY_STATUSES',
        'target_table': 'STG_CSF_WF_ITEM_ACTIVITY_STATUSES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.982658+00:00'
    }
) }}

WITH 

source_csf_wf_item_activity_statuses AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        item_type,
        item_key,
        process_activity,
        activity_status,
        activity_result_code,
        assigned_user,
        notification_id,
        begin_date,
        end_date,
        execution_time,
        error_name,
        error_message,
        error_stack,
        outbound_queue_id,
        due_date,
        security_group_id,
        action,
        performed_by,
        inst_id
    FROM {{ source('raw', 'csf_wf_item_activity_statuses') }}
),

source_stg_csf_wf_item_activity_statuses AS (
    SELECT
        item_type,
        item_key,
        process_activity,
        activity_status,
        activity_result_code,
        assigned_user,
        notification_id,
        begin_date,
        end_date,
        execution_time,
        error_name,
        error_message,
        error_stack,
        outbound_queue_id,
        due_date,
        security_group_id,
        action,
        performed_by,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_wf_item_activity_statuses') }}
),

transformed_exp_csf_wf_item_activity_statuses AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    item_type,
    item_key,
    process_activity,
    activity_status,
    activity_result_code,
    assigned_user,
    notification_id,
    begin_date,
    end_date,
    execution_time,
    error_name,
    error_message,
    error_stack,
    outbound_queue_id,
    due_date,
    security_group_id,
    action,
    performed_by,
    inst_id
    FROM source_stg_csf_wf_item_activity_statuses
),

final AS (
    SELECT
        item_type,
        item_key,
        process_activity,
        activity_status,
        activity_result_code,
        assigned_user,
        notification_id,
        begin_date,
        end_date,
        execution_time,
        error_name,
        error_message,
        error_stack,
        outbound_queue_id,
        due_date,
        security_group_id,
        action,
        performed_by,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_wf_item_activity_statuses
)

SELECT * FROM final