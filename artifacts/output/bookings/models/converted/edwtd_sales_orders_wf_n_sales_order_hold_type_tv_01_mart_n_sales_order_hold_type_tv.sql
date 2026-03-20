{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_hold_type_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_HOLD_TYPE_TV',
        'target_table': 'N_SALES_ORDER_HOLD_TYPE_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.992270+00:00'
    }
) }}

WITH 

source_w_sales_order_hold_type AS (
    SELECT
        bk_sol_hold_type_code,
        start_tv_date,
        end_tv_date,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_order_hold_type') }}
),

final AS (
    SELECT
        bk_sol_hold_type_code,
        start_tv_date,
        end_tv_date,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM source_w_sales_order_hold_type
)

SELECT * FROM final