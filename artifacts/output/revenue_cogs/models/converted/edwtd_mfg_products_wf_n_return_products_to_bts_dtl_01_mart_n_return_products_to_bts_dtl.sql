{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_return_products_to_bts_dtl', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_RETURN_PRODUCTS_TO_BTS_DTL',
        'target_table': 'N_RETURN_PRODUCTS_TO_BTS_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.547384+00:00'
    }
) }}

WITH 

source_w_return_products_to_bts_dtl AS (
    SELECT
        return_products_to_bts_dtl_key,
        ship_dt,
        product_qty,
        create_dtm,
        dv_create_dt,
        return_reason_cd,
        process_status_cd,
        sk_message_id,
        sk_pid_line_id_int,
        return_products_to_bts_key,
        product_inventory_org_key,
        product_item_key,
        bk_carton_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_return_products_to_bts_dtl') }}
),

final AS (
    SELECT
        return_products_to_bts_dtl_key,
        ship_dt,
        product_qty,
        create_dtm,
        dv_create_dt,
        return_reason_cd,
        process_status_cd,
        sk_message_id,
        sk_pid_line_id_int,
        return_products_to_bts_key,
        product_inventory_org_key,
        product_item_key,
        bk_carton_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_return_products_to_bts_dtl
)

SELECT * FROM final