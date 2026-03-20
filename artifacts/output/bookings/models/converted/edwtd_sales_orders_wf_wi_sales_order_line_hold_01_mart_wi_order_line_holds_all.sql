{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_order_line_hold', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_ORDER_LINE_HOLD',
        'target_table': 'WI_ORDER_LINE_HOLDS_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.090147+00:00'
    }
) }}

WITH 

source_st_cg1_oe_order_holds_all AS (
    SELECT
        order_hold_id,
        attribute1,
        attribute3,
        creation_date,
        header_id,
        released_flag,
        hold_release_id,
        hold_source_id,
        line_id,
        attribute13,
        attribute10,
        attribute11,
        attribute12,
        attribute14,
        attribute15,
        attribute2,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        context,
        created_by,
        last_updated_by,
        last_update_date,
        last_update_login,
        org_id,
        program_application_id,
        program_id,
        program_update_date,
        request_id,
        source_commit_time,
        create_datetime,
        global_name,
        source_dml_type
    FROM {{ source('raw', 'st_cg1_oe_order_holds_all') }}
),

source_wi_order_line_holds_all_nrt AS (
    SELECT
        order_hold_id,
        header_id,
        line_id,
        hold_source_id,
        hold_release_id,
        global_name,
        create_datetime,
        update_datetime,
        edw_create_user,
        attribute1,
        hold_auto_release_dt_txt,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        org_id,
        edw_update_user,
        source_deleted_flg
    FROM {{ source('raw', 'wi_order_line_holds_all_nrt') }}
),

source_ex_oe_order_holds_all_nrt AS (
    SELECT
        order_hold_id,
        attribute1,
        creation_date,
        header_id,
        hold_release_id,
        hold_source_id,
        line_id,
        hold_auto_release_dt_txt,
        created_by,
        last_updated_by,
        last_update_date,
        org_id,
        global_name,
        create_datetime,
        exception_type
    FROM {{ source('raw', 'ex_oe_order_holds_all_nrt') }}
),

source_wi_sales_order_line_hold_nrt AS (
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
    FROM {{ source('raw', 'wi_sales_order_line_hold_nrt') }}
),

source_wi_order_holds_all_nrt AS (
    SELECT
        order_hold_id,
        header_id,
        line_id,
        hold_source_id,
        hold_release_id,
        global_name,
        create_datetime,
        update_datetime,
        edw_create_user,
        attribute1,
        hold_auto_release_dt_txt,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        org_id,
        edw_update_user,
        source_deleted_flg
    FROM {{ source('raw', 'wi_order_holds_all_nrt') }}
),

final AS (
    SELECT
        order_hold_id,
        header_id,
        line_id,
        hold_source_id,
        hold_release_id,
        global_name,
        create_datetime,
        update_datetime,
        edw_create_user,
        attribute1,
        hold_auto_release_dt_txt,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        org_id,
        edw_update_user,
        source_deleted_flg
    FROM source_wi_order_holds_all_nrt
)

SELECT * FROM final