{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cs_sales_order_task_type ', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CS_SALES_ORDER_TASK_TYPE ',
        'target_table': 'N_CS_SALES_ORDER_TASK_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.248137+00:00'
    }
) }}

WITH 

source_w_cs_sales_order_task_type AS (
    SELECT
        bk_cs_sales_order_task_type_cd,
        cs_sales_order_task_type_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_cs_sales_order_task_type') }}
),

final AS (
    SELECT
        bk_cs_sales_order_task_type_cd,
        cs_sales_order_task_type_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_cs_sales_order_task_type
)

SELECT * FROM final