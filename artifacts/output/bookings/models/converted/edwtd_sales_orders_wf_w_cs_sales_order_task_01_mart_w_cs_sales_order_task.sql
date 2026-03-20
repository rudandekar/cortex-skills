{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_cs_sales_order_task', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_CS_SALES_ORDER_TASK',
        'target_table': 'W_CS_SALES_ORDER_TASK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.992516+00:00'
    }
) }}

WITH 

source_wi_cs_so_task_line AS (
    SELECT
        cs_sales_order_task_key,
        sales_order_key,
        bk_cs_sales_order_task_type_cd,
        cs_so_task_status_cd,
        cs_so_task_created_dtm,
        sk_order_task_id_int,
        ss_cd,
        cs_so_task_obsolete_flg,
        cs_sales_order_task_txt,
        order_number,
        ec_header_id,
        erp_header_id,
        ec_line_id,
        erp_line_id
    FROM {{ source('raw', 'wi_cs_so_task_line') }}
),

final AS (
    SELECT
        cs_sales_order_task_key,
        ru_sales_order_line_key,
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_wi_cs_so_task_line
)

SELECT * FROM final