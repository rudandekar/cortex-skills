{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_cs_sales_order_task_cec', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_CS_SALES_ORDER_TASK_CEC',
        'target_table': 'ST_OM_CEC_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.575106+00:00'
    }
) }}

WITH 

source_st_om_cec_lines AS (
    SELECT
        creation_date,
        ec_header_id,
        ec_line_id,
        ec_line_number,
        created_by,
        ec_batch_id,
        ec_parent_line_id,
        ec_link_to_line_id,
        erp_line_id,
        inventory_item,
        inventory_item_id,
        item_type_code,
        last_update_date,
        last_update_login,
        last_updated_by,
        warehouse,
        warehouse_id,
        original_system_line_id,
        org_id,
        source_line_id,
        erp_header_id,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cec_lines') }}
),

source_ex_om_cec_lines AS (
    SELECT
        creation_date,
        ec_header_id,
        ec_line_id,
        ec_line_number,
        created_by,
        ec_batch_id,
        ec_parent_line_id,
        ec_link_to_line_id,
        erp_line_id,
        inventory_item,
        inventory_item_id,
        item_type_code,
        last_update_date,
        last_update_login,
        last_updated_by,
        warehouse,
        warehouse_id,
        original_system_line_id,
        org_id,
        source_line_id,
        erp_header_id,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_om_cec_lines') }}
),

source_ex_om_cec_tasks AS (
    SELECT
        task_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        task_code,
        task_number,
        task_name,
        task_type_code,
        required_for_entry_flag,
        error_task_flag,
        last_update_login,
        table_name,
        column_name,
        clear_when_invalid_flag,
        task_text,
        change_order_error_code,
        task_explanation,
        consolidate_flag,
        auto_delete_flag,
        for_solcat,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_om_cec_tasks') }}
),

source_st_om_cec_tasks AS (
    SELECT
        task_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        task_code,
        task_number,
        task_name,
        task_type_code,
        required_for_entry_flag,
        error_task_flag,
        last_update_login,
        table_name,
        column_name,
        clear_when_invalid_flag,
        task_text,
        change_order_error_code,
        task_explanation,
        consolidate_flag,
        auto_delete_flag,
        for_solcat,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cec_tasks') }}
),

source_ex_om_cec_headers AS (
    SELECT
        order_number,
        ec_header_id,
        global_name,
        erp_header_id,
        ges_update_date,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        load_code,
        operation_code,
        batch_id,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_om_cec_headers') }}
),

source_st_om_cec_headers AS (
    SELECT
        order_number,
        ec_header_id,
        global_name,
        erp_header_id,
        ges_update_date,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        load_code,
        operation_code,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cec_headers') }}
),

source_ex_om_cec_order_tasks AS (
    SELECT
        created_by,
        creation_date,
        ec_header_id,
        ec_line_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        notes,
        obsolete_flag,
        order_task_id,
        original_system_reference,
        original_system_source_name,
        task_id,
        task_status_code,
        task_text,
        batch_id,
        create_datetime,
        action_code,
        exception_type
    FROM {{ source('raw', 'ex_om_cec_order_tasks') }}
),

source_st_om_cec_order_tasks AS (
    SELECT
        created_by,
        creation_date,
        ec_header_id,
        ec_line_id,
        ges_update_date,
        global_name,
        last_updated_by,
        last_update_date,
        last_update_login,
        notes,
        obsolete_flag,
        order_task_id,
        original_system_reference,
        original_system_source_name,
        task_id,
        task_status_code,
        task_text,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_om_cec_order_tasks') }}
),

final AS (
    SELECT
        creation_date,
        ec_header_id,
        ec_line_id,
        ec_line_number,
        created_by,
        ec_batch_id,
        ec_parent_line_id,
        ec_link_to_line_id,
        erp_line_id,
        inventory_item,
        inventory_item_id,
        item_type_code,
        last_update_date,
        last_update_login,
        last_updated_by,
        warehouse,
        warehouse_id,
        original_system_line_id,
        org_id,
        source_line_id,
        erp_header_id,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM source_st_om_cec_order_tasks
)

SELECT * FROM final