{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_opl_sales_order_hold', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_WI_OPL_SALES_ORDER_HOLD',
        'target_table': 'EX_XXOPL_ORDER_HOLDS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.791275+00:00'
    }
) }}

WITH 

source_sm_sales_order_hold AS (
    SELECT
        sales_order_hold_key,
        sk_order_hold_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_sales_order_hold') }}
),

source_wi_opl_ord_holds_data_all AS (
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
    FROM {{ source('raw', 'wi_opl_ord_holds_data_all') }}
),

source_wi_opl_sales_order_hold AS (
    SELECT
        sales_order_hold_key,
        bk_so_hold_dtm,
        bk_sales_order_hold_name,
        bk_sales_order_key,
        created_by_id_int,
        sk_order_hold_id_int,
        ss_cd,
        last_updated_by_int,
        so_released_role,
        so_hold_reason_txt,
        so_hold_crtd_by_erp_user_name,
        so_hold_upd_by_erp_user_name,
        ru_sk_hold_release_id_int,
        ru_so_hold_release_reason_cd,
        ru_so_hold_release_dt,
        ru_so_hld_rlsd_by_erp_usr_name,
        ru_so_hold_release_dtm,
        dv_so_hold_dt,
        edw_create_user,
        edw_create_datetime,
        so_hold_auto_release_dt,
        release_comment,
        hold_comment,
        hold_until_dt
    FROM {{ source('raw', 'wi_opl_sales_order_hold') }}
),

source_ex_xxopl_order_holds_all AS (
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
    FROM {{ source('raw', 'ex_xxopl_order_holds_all') }}
),

final AS (
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
    FROM source_ex_xxopl_order_holds_all
)

SELECT * FROM final