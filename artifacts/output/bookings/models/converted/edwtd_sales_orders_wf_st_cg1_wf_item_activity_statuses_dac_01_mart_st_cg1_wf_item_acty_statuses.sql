{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_item_activity_statuses_dac', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_WF_ITEM_ACTIVITY_STATUSES_DAC',
        'target_table': 'ST_CG1_WF_ITEM_ACTY_STATUSES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.770857+00:00'
    }
) }}

WITH 

source_cg1_wf_item_activity_statuses AS (
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
    FROM {{ source('raw', 'cg1_wf_item_activity_statuses') }}
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
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM source_cg1_wf_item_activity_statuses
)

SELECT * FROM final