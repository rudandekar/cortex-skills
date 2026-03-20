{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_line_hold', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_LINE_HOLD',
        'target_table': 'W_SALES_ORDER_LINE_HOLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.775535+00:00'
    }
) }}

WITH 

source_wi_sales_order_line_hold AS (
    SELECT
        sales_order_line_hold_key,
        bk_sol_hold_datetime,
        bk_sales_order_hold_name,
        sales_order_line_key,
        created_by_id_int,
        released_role,
        ss_code,
        sk_order_hold_id_int,
        last_updated_by_int,
        ru_sol_hold_release_datetime,
        ru_sol_hold_release_reason_cd,
        edw_create_user,
        edw_create_datetime,
        sol_hold_crtd_by_erp_user_name,
        sol_hold_upd_by_erp_user_name,
        sales_line_hold_reason_txt,
        sol_hold_rlsd_by_erp_user_name,
        dd_sales_order_key,
        ru_sk_hold_release_id_int,
        ru_dv_sol_hold_release_dt,
        dv_sol_hold_dt,
        sol_hold_auto_release_dt,
        hold_comment,
        release_comment,
        hold_until_dt
    FROM {{ source('raw', 'wi_sales_order_line_hold') }}
),

transformed_exp_w_sales_order_line_hold AS (
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
    action_code,
    rank_index,
    dml_type,
    sol_hold_auto_release_dt,
    so_hold_comment,
    ru_so_release_comment,
    bk_so_hold_type_reason_cd,
    hold_until_dt
    FROM source_wi_sales_order_line_hold
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
        action_code,
        dml_type,
        hold_until_dt
    FROM transformed_exp_w_sales_order_line_hold
)

SELECT * FROM final