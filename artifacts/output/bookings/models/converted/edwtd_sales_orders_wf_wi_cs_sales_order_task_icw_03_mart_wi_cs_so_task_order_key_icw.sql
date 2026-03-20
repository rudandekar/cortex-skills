{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_cs_sales_order_task_icw', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_CS_SALES_ORDER_TASK_ICW',
        'target_table': 'WI_CS_SO_TASK_ORDER_KEY_ICW',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.702631+00:00'
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

source_st_om_xxccp_oe_order_header AS (
    SELECT
        xxccp_header_id,
        quote_number,
        header_id,
        global_name,
        ges_update_date,
        status,
        sub_status,
        order_number,
        order_source_id,
        cs_case_number,
        cs_request_id,
        context,
        ordered_date,
        order_type_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        operation,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_xxccp_oe_order_header') }}
),

source_st_om_xxccp_error_log AS (
    SELECT
        error_log_id,
        last_updated_by,
        last_update_date,
        created_by,
        creation_date,
        last_update_login_id,
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
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_xxccp_error_log') }}
),

final AS (
    SELECT
        cs_sales_order_task_key,
        ru_sales_order_key,
        bk_cs_sales_order_task_type_cd,
        cs_so_task_status_cd,
        cs_sales_order_task_type,
        cs_so_task_created_dtm,
        sk_order_task_id_int,
        ss_cd,
        ss_table_cd,
        cs_so_task_obsolete_flg,
        cs_sales_order_task_txt,
        order_line_id
    FROM source_st_om_xxccp_error_log
)

SELECT * FROM final