{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ccw_nt_flag_cg1', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_CCW_NT_FLAG_CG1',
        'target_table': 'WI_CCW_NT_FLAG_CG1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.438546+00:00'
    }
) }}

WITH 

source_st_cg1_xxccp_error_log_nt AS (
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
    FROM {{ source('raw', 'st_cg1_xxccp_error_log_nt') }}
),

final AS (
    SELECT
        sales_order_key,
        header_id,
        global_name,
        no_touch_flag,
        quote_number,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date
    FROM source_st_cg1_xxccp_error_log_nt
)

SELECT * FROM final