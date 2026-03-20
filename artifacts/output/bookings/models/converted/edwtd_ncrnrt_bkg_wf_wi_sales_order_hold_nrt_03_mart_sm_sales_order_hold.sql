{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_order_hold_nrt', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_ORDER_HOLD_NRT',
        'target_table': 'SM_SALES_ORDER_HOLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.206854+00:00'
    }
) }}

WITH 

source_st_cg1_oe_order_holds_all_nrt AS (
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
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_cg1_oe_order_holds_all_nrt') }}
),

source_wi_sales_order_hold_nrt AS (
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
        hold_comment
    FROM {{ source('raw', 'wi_sales_order_hold_nrt') }}
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

final AS (
    SELECT
        sales_order_hold_key,
        sk_order_hold_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user
    FROM source_ex_oe_order_holds_all_nrt
)

SELECT * FROM final