{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_line_hold_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_LINE_HOLD_TV',
        'target_table': 'N_SALES_ORDER_LINE_HOLD_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.903497+00:00'
    }
) }}

WITH 

source_w_sales_order_line_hold AS (
    SELECT
        sales_order_line_hold_key,
        start_tv_date,
        end_tv_date,
        bk_sol_hold_datetime,
        bk_sales_order_hold_name,
        sales_order_line_key,
        released_role,
        ss_code,
        sk_order_hold_id_int,
        ru_sol_hold_release_datetime,
        ru_sol_hold_release_reason_cd,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        sol_hold_crtd_by_erp_user_name,
        sol_hold_upd_by_erp_user_name,
        sales_line_hold_reason_txt,
        sol_hold_rlsd_by_erp_user_name,
        dd_sales_order_key,
        ru_sk_hold_release_id_int,
        ru_dv_sol_hold_release_dt,
        dv_sol_hold_dt,
        sol_hold_auto_release_dt,
        sol_hold_comment,
        ru_sol_release_comment,
        bk_so_hold_type_reason_cd,
        action_code,
        dml_type,
        hold_until_dt
    FROM {{ source('raw', 'w_sales_order_line_hold') }}
),

final AS (
    SELECT
        sales_order_line_hold_key,
        start_tv_date,
        end_tv_date,
        bk_sol_hold_datetime,
        bk_sales_order_hold_name,
        sales_order_line_key,
        released_role,
        ss_code,
        sk_order_hold_id_int,
        ru_sol_hold_release_datetime,
        ru_sol_hold_release_reason_cd,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime,
        sol_hold_crtd_by_erp_user_name,
        sol_hold_upd_by_erp_user_name,
        sales_line_hold_reason_txt,
        sol_hold_rlsd_by_erp_user_name,
        dd_sales_order_key,
        ru_sk_hold_release_id_int,
        ru_dv_sol_hold_release_dt,
        dv_sol_hold_dt,
        sol_hold_auto_release_dt,
        sol_hold_comment,
        ru_sol_release_comment,
        bk_so_hold_type_reason_cd,
        hold_until_dt
    FROM source_w_sales_order_line_hold
)

SELECT * FROM final