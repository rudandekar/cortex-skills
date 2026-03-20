{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_cs_sales_order_task_type', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_CS_SALES_ORDER_TASK_TYPE',
        'target_table': 'W_CS_SALES_ORDER_TASK_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.519341+00:00'
    }
) }}

WITH 

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

final AS (
    SELECT
        bk_cs_sales_order_task_type_cd,
        cs_sales_order_task_type_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_om_cec_tasks
)

SELECT * FROM final