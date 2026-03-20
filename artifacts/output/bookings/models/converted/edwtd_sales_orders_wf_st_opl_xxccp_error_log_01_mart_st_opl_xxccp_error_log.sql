{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_opl_xxccp_error_log', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OPL_XXCCP_ERROR_LOG',
        'target_table': 'ST_OPL_XXCCP_ERROR_LOG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.056310+00:00'
    }
) }}

WITH 

source_st_opl_xxccp_error_log AS (
    SELECT
        error_log_id,
        xxccp_header_id,
        xxccp_line_id,
        order_header_id,
        order_line_id,
        error_message,
        error_description,
        error_date,
        error_type,
        error_status,
        for_cse_integration,
        global_name,
        last_update_date,
        creation_date,
        saas_error,
        source_commit_time
    FROM {{ source('raw', 'st_opl_xxccp_error_log') }}
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
        error_type,
        error_status,
        for_cse_integration,
        global_name,
        last_update_date,
        creation_date,
        saas_error,
        source_commit_time
    FROM source_st_opl_xxccp_error_log
)

SELECT * FROM final