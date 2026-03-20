{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_product_hold', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_PRODUCT_HOLD',
        'target_table': 'N_PRODUCT_HOLD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.409984+00:00'
    }
) }}

WITH 

source_w_product_hold AS (
    SELECT
        product_hold_key,
        product_hold_reason_txt,
        bk_product_hold_dtm,
        dv_product_hold_dt,
        bk_product_key,
        bk_product_hold_name,
        released_role,
        sk_hold_source_id_int,
        ss_cd,
        prdt_hold_created_erp_usr_name,
        prdt_hold_updated_erp_usr_name,
        operating_unit_name_cd,
        ru_hold_release_reason_cd,
        ru_hold_release_dtm,
        ru_dv_hold_release_dt,
        ru_hold_released_erp_user_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        product_hold_comment,
        compliance_hold_role,
        ru_compliance_hold_start_dt,
        ru_compliance_hold_cmt,
        ru_cmplc_hld_csco_wkr_prty_key,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_product_hold') }}
),

final AS (
    SELECT
        product_hold_key,
        product_hold_reason_txt,
        bk_product_hold_dtm,
        dv_product_hold_dt,
        bk_product_key,
        bk_product_hold_name,
        released_role,
        sk_hold_source_id_int,
        ss_cd,
        prdt_hold_created_erp_usr_name,
        prdt_hold_updated_erp_usr_name,
        operating_unit_name_cd,
        ru_hold_release_reason_cd,
        ru_hold_release_dtm,
        ru_dv_hold_release_dt,
        ru_hold_released_erp_user_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        product_hold_comment,
        compliance_hold_role,
        ru_compliance_hold_start_dt,
        ru_compliance_hold_cmt,
        ru_cmplc_hld_csco_wkr_prty_key
    FROM source_w_product_hold
)

SELECT * FROM final