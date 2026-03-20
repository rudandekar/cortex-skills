{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_hold_def', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_HOLD_DEF',
        'target_table': 'W_SALES_ORDER_HOLD_DEF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.399490+00:00'
    }
) }}

WITH 

source_st_uo_oe_hold_definitions AS (
    SELECT
        hold_id,
        activity_name,
        name,
        attribute2,
        description,
        type_code,
        attribute6,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        create_datetime
    FROM {{ source('raw', 'st_uo_oe_hold_definitions') }}
),

transformed_exp_uo_oe_hold_definitions AS (
    SELECT
    bk_sales_order_hold_name,
    start_tv_date,
    end_tv_date,
    sales_order_line_hold_descr,
    bk_sol_hold_type_code,
    bk_sol_hold_group_code,
    r12_so_hold_short_descr,
    shipping_hold_flg,
    manufacturing_hold_flg,
    move_out_window_days_cnt,
    move_out_days_cnt,
    move_out_hold_flg,
    send_hold_to_mpa_flg,
    action_code,
    rank_index,
    dml_type
    FROM source_st_uo_oe_hold_definitions
),

final AS (
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
        send_hold_to_mpa_flg,
        action_code,
        dml_type
    FROM transformed_exp_uo_oe_hold_definitions
)

SELECT * FROM final