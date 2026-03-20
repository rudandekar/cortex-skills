{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_hold_def', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_HOLD_DEF',
        'target_table': 'N_SALES_ORDER_HOLD_DEF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.808380+00:00'
    }
) }}

WITH 

source_n_sales_order_hold_def_tv AS (
    SELECT
        bk_sales_order_hold_name,
        start_tv_date,
        end_tv_date,
        sales_order_line_hold_descr,
        bk_sol_hold_type_code,
        bk_sol_hold_group_code,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        r12_so_hold_short_descr,
        shipping_hold_flg,
        manufacturing_hold_flg,
        move_out_window_days_cnt,
        move_out_days_cnt,
        move_out_hold_flg,
        send_hold_to_mpa_flg
    FROM {{ source('raw', 'n_sales_order_hold_def_tv') }}
),

final AS (
    SELECT
        bk_sales_order_hold_name,
        sales_order_line_hold_descr,
        bk_sol_hold_type_code,
        bk_sol_hold_group_code,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        r12_so_hold_short_descr,
        shipping_hold_flg,
        manufacturing_hold_flg,
        move_out_window_days_cnt,
        move_out_days_cnt,
        move_out_hold_flg,
        send_hold_to_mpa_flg
    FROM source_n_sales_order_hold_def_tv
)

SELECT * FROM final