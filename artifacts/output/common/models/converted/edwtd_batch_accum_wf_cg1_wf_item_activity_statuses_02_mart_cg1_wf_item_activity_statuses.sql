{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_item_activity_statuses', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_WF_ITEM_ACTIVITY_STATUSES',
        'target_table': 'CG1_WF_ITEM_ACTIVITY_STATUSES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.849914+00:00'
    }
) }}

WITH 

source_cg1_wf_item_activity_statuses AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
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
    FROM {{ source('raw', 'cg1_wf_item_activity_statuses') }}
),

source_stg_cg1_wf_itm_actvty_statuses AS (
    SELECT
        action,
        activity_result_code,
        activity_status,
        assigned_user,
        begin_date,
        due_date,
        end_date,
        error_message,
        error_name,
        error_stack,
        execution_time,
        item_key,
        item_type,
        notification_id,
        performed_by,
        process_activity,
        security_group_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_wf_itm_actvty_statuses') }}
),

transformed_exp_cg1_wf_item_activity_statuses AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
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
    due_date,
    security_group_id,
    action,
    performed_by
    FROM source_stg_cg1_wf_itm_actvty_statuses
),

final AS (
    SELECT
        action,
        activity_result_code,
        activity_status,
        assigned_user,
        begin_date,
        due_date,
        end_date,
        error_message,
        error_name,
        error_stack,
        execution_time,
        item_key,
        item_type,
        notification_id,
        performed_by,
        process_activity,
        security_group_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_wf_item_activity_statuses
)

SELECT * FROM final