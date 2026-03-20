{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_return_items_shipment', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_RETURN_ITEMS_SHIPMENT',
        'target_table': 'W_RETURN_ITEMS_SHIPMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.404251+00:00'
    }
) }}

WITH 

source_st_cg1_x_mk_3c8_ln_ob_iface AS (
    SELECT
        message_id,
        line_iface_id,
        transmission_date,
        holding_unit_id,
        holding_unit_ref,
        line_number,
        item_number,
        item_revision,
        product_quantity,
        return_reason_code,
        ship_date,
        uom,
        pallet_id,
        process_status,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        request_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        expected_arrival_date,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_x_mk_3c8_ln_ob_iface') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_cg1_x_mk_3c8_ln_ob_iface
)

SELECT * FROM final