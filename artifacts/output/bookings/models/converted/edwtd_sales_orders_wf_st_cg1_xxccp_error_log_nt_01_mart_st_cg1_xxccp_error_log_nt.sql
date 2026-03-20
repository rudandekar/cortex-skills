{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_xxccp_error_log_nt', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_XXCCP_ERROR_LOG_NT',
        'target_table': 'ST_CG1_XXCCP_ERROR_LOG_NT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.461755+00:00'
    }
) }}

WITH 

source_cg1_xxccp_error_log AS (
    SELECT
        error_log_id,
        xxccp_header_id,
        xxccp_line_id,
        quote_header_id,
        quote_line_id,
        web_order_number,
        order_header_id,
        order_line_id,
        error_message,
        error_description,
        error_date,
        error_type,
        error_status,
        for_cse_integration,
        error_stage,
        error_sub_stage,
        operation_code,
        last_updated_by,
        last_update_date,
        created_by,
        creation_date,
        last_update_login_id,
        change_history_id,
        entity,
        element_name,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag,
        saas_error
    FROM {{ source('raw', 'cg1_xxccp_error_log') }}
),

final AS (
    SELECT
        error_log_id,
        xxccp_header_id,
        xxccp_line_id,
        order_header_id,
        order_line_id,
        error_message,
        error_description,
        error_date,
        error_status,
        for_cse_integration,
        operation_code,
        global_name,
        last_updated_by,
        last_update_date,
        created_by,
        creation_date,
        source_commit_time
    FROM source_cg1_xxccp_error_log
)

SELECT * FROM final