{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_cancelled_sol', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_CANCELLED_SOL',
        'target_table': 'N_CANCELLED_SOL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.401712+00:00'
    }
) }}

WITH 

source_w_cancelled_sol AS (
    SELECT
        sales_order_line_key,
        sol_cancel_reason_code,
        bk_so_line_cancel_reason_cd,
        sol_cancelled_datetime,
        sol_cancelled_by_int,
        sol_cancelled_quantity,
        sol_order_quantity,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_cancelled_sol') }}
),

final AS (
    SELECT
        sales_order_line_key,
        sol_cancel_reason_code,
        bk_so_line_cancel_reason_cd,
        sol_cancelled_datetime,
        sol_cancelled_by_int,
        sol_cancelled_quantity,
        sol_order_quantity,
        ss_code,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        bk_ship_set_num_int,
        sales_order_key
    FROM source_w_cancelled_sol
)

SELECT * FROM final