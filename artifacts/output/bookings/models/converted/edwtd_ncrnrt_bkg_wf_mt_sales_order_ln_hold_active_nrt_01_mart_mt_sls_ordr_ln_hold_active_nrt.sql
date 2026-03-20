{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sales_order_ln_hold_active_nrt', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_MT_SALES_ORDER_LN_HOLD_ACTIVE_NRT',
        'target_table': 'MT_SLS_ORDR_LN_HOLD_ACTIVE_NRT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.851089+00:00'
    }
) }}

WITH 

source_n_sales_order_line_hold_nrt AS (
    SELECT
        sales_order_line_hold_key,
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
        ru_sol_release_comment
    FROM {{ source('raw', 'n_sales_order_line_hold_nrt') }}
),

final AS (
    SELECT
        sales_order_line_hold_key,
        sales_order_hold_key,
        bk_sol_hold_datetime,
        bk_sales_order_hold_name,
        sales_order_line_key,
        sales_order_key,
        so_hold_flg,
        sol_hold_crtd_by_erp_user_name,
        sol_hold_upd_by_erp_user_name,
        sales_line_hold_reason_txt,
        edw_create_user,
        edw_create_datetime
    FROM source_n_sales_order_line_hold_nrt
)

SELECT * FROM final