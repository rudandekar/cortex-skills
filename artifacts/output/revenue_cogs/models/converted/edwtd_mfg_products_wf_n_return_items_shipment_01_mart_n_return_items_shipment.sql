{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_return_items_shipment', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_RETURN_ITEMS_SHIPMENT',
        'target_table': 'N_RETURN_ITEMS_SHIPMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.729072+00:00'
    }
) }}

WITH 

source_w_return_items_shipment AS (
    SELECT
        bk_carton_id,
        bk_purchase_order_num,
        process_status_cd,
        return_reason_cd,
        create_dt,
        ship_dt,
        product_qty,
        transmission_dt,
        expected_arrival_dt,
        rtrn_itms_frmslc_mfg_prtnr_key,
        product_key,
        sk_message_id,
        sk_line_iface_id_int,
        purchase_order_line_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_return_items_shipment') }}
),

final AS (
    SELECT
        bk_carton_id,
        bk_purchase_order_num,
        process_status_cd,
        return_reason_cd,
        create_dt,
        ship_dt,
        product_qty,
        transmission_dt,
        expected_arrival_dt,
        rtrn_itms_frmslc_mfg_prtnr_key,
        product_key,
        sk_message_id,
        sk_line_iface_id_int,
        purchase_order_line_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_return_items_shipment
)

SELECT * FROM final