{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_line_dels', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_LINE_DELS',
        'target_table': 'WI_SALES_ORDER_LINE_DELS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.798036+00:00'
    }
) }}

WITH 

source_st_bo_oe_order_lines_all_dels AS (
    SELECT
        line_id,
        queue_id,
        action_code,
        error_msg,
        table_name,
        de_name,
        process_set_id,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'st_bo_oe_order_lines_all_dels') }}
),

source_st_uo_oe_order_lines_all_dels AS (
    SELECT
        line_id,
        queue_id,
        action_code,
        error_msg,
        table_name,
        de_name,
        process_set_id,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'st_uo_oe_order_lines_all_dels') }}
),

final AS (
    SELECT
        sales_order_line_key,
        edw_create_user,
        edw_create_datetime
    FROM source_st_uo_oe_order_lines_all_dels
)

SELECT * FROM final