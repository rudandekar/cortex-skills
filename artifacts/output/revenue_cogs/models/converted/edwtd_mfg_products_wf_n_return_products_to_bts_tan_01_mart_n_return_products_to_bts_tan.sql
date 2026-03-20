{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_return_products_to_bts_tan', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_RETURN_PRODUCTS_TO_BTS_TAN',
        'target_table': 'N_RETURN_PRODUCTS_TO_BTS_TAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.154431+00:00'
    }
) }}

WITH 

source_w_return_products_to_bts_tan AS (
    SELECT
        return_products_to_bts_tan_key,
        process_status_cd,
        create_dtm,
        dv_create_dt,
        return_products_to_bts_dtl_key,
        sk_message_id,
        sk_pid_line_id_int,
        sk_tan_id_int,
        tan_inventory_organization_key,
        tan_item_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_return_products_to_bts_tan') }}
),

final AS (
    SELECT
        return_products_to_bts_tan_key,
        process_status_cd,
        create_dtm,
        dv_create_dt,
        return_products_to_bts_dtl_key,
        sk_message_id,
        sk_pid_line_id_int,
        sk_tan_id_int,
        tan_inventory_organization_key,
        tan_item_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_return_products_to_bts_tan
)

SELECT * FROM final