{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_opl_sales_ord_line_hold_nrt', 'batch', 'edwtd_xaas_bkgs_nrt'],
    meta={
        'source_workflow': 'wf_m_WI_OPL_SALES_ORD_LINE_HOLD_NRT',
        'target_table': 'WI_OPL_SALES_ORD_LINE_HOLD_NRT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.344124+00:00'
    }
) }}

WITH 

source_sm_sales_order_line_hold AS (
    SELECT
        sales_order_line_hold_key,
        sk_order_hold_id_int,
        sk_line_id_int,
        ss_code,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_sales_order_line_hold') }}
),

source_ex_xxopl_order_holds_all_nrt AS (
    SELECT
        order_hold_id,
        creation_date,
        hold_name,
        header_id,
        line_id,
        hold_id,
        released_flag,
        hold_apply_reason,
        created_by,
        last_updated_by,
        hold_release_reason,
        hold_release_date,
        hold_released_by,
        hold_apply_comment,
        hold_release_comment,
        ss_cd,
        global_name,
        create_datetime,
        exception_type
    FROM {{ source('raw', 'ex_xxopl_order_holds_all_nrt') }}
),

source_wi_opl_sales_ord_line_hold_nrt AS (
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
        release_comment
    FROM {{ source('raw', 'wi_opl_sales_ord_line_hold_nrt') }}
),

source_wi_opl_ord_line_holds_all_nrt AS (
    SELECT
        order_hold_id,
        creation_date,
        hold_name,
        header_id,
        line_id,
        hold_id,
        released_flag,
        hold_apply_reason,
        created_by,
        last_updated_by,
        hold_release_reason,
        hold_release_date,
        hold_released_by,
        hold_apply_comment,
        hold_release_comment,
        ss_cd,
        global_name,
        create_datetime,
        update_datetime,
        source_deleted_flg
    FROM {{ source('raw', 'wi_opl_ord_line_holds_all_nrt') }}
),

final AS (
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
        release_comment
    FROM source_wi_opl_ord_line_holds_all_nrt
)

SELECT * FROM final