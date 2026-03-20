{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_demand_class', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_DEMAND_CLASS',
        'target_table': 'N_SALES_ORDER_DEMAND_CLASS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.570896+00:00'
    }
) }}

WITH 

source_w_sales_order_demand_class AS (
    SELECT
        bk_demand_class_cd,
        demand_class_descr,
        demand_class_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_order_demand_class') }}
),

final AS (
    SELECT
        bk_demand_class_cd,
        demand_class_descr,
        demand_class_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_sales_order_demand_class
)

SELECT * FROM final