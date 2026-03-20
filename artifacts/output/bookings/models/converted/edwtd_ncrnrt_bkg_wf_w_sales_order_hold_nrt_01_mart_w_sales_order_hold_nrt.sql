{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_hold_nrt', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_HOLD_NRT',
        'target_table': 'W_SALES_ORDER_HOLD_NRT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.707305+00:00'
    }
) }}

WITH 

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

transformed_exp_w_sales_order_hold AS (
    SELECT
    sales_order_hold_key,
    bk_so_hold_dtm,
    bk_sales_order_hold_name,
    bk_sales_order_key,
    sk_order_hold_id_int,
    ss_cd,
    so_released_role,
    so_hold_reason_txt,
    so_hold_crtd_by_erp_user_name,
    so_hold_upd_by_erp_user_name,
    ru_sk_hold_release_id_int,
    ru_so_hold_release_reason_cd,
    ru_so_hold_release_dt,
    ru_so_hld_rlsd_by_erp_usr_name,
    edw_create_dtm,
    edw_create_user,
    edw_update_dtm,
    edw_update_user,
    ru_so_hold_release_dtm,
    dv_so_hold_dt,
    so_hold_auto_release_dt,
    so_hold_comment,
    ru_so_release_comment,
    start_tv_dtm,
    end_tv_dtm,
    action_code,
    rank_index,
    dml_type
    FROM source_wi_sales_order_hold_nrt
),

final AS (
    SELECT
        sales_order_hold_key,
        bk_so_hold_dtm,
        bk_sales_order_hold_name,
        bk_sales_order_key,
        sk_order_hold_id_int,
        ss_cd,
        so_released_role,
        so_hold_reason_txt,
        so_hold_crtd_by_erp_user_name,
        so_hold_upd_by_erp_user_name,
        ru_sk_hold_release_id_int,
        ru_so_hold_release_reason_cd,
        ru_so_hold_release_dt,
        ru_so_hld_rlsd_by_erp_usr_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ru_so_hold_release_dtm,
        dv_so_hold_dt,
        so_hold_auto_release_dt,
        so_hold_comment,
        ru_so_release_comment,
        start_tv_dtm,
        end_tv_dtm,
        action_code,
        dml_type
    FROM transformed_exp_w_sales_order_hold
)

SELECT * FROM final