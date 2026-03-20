{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cs_sales_order_task', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CS_SALES_ORDER_TASK',
        'target_table': 'N_CS_SALES_ORDER_TASK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.435812+00:00'
    }
) }}

WITH 

source_w_cs_sales_order_task AS (
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
    FROM {{ source('raw', 'w_cs_sales_order_task') }}
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
        edw_update_user
    FROM source_w_cs_sales_order_task
)

SELECT * FROM final