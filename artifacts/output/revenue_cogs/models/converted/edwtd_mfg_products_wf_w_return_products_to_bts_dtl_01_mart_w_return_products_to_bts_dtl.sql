{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_return_products_to_bts_dtl', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_RETURN_PRODUCTS_TO_BTS_DTL',
        'target_table': 'W_RETURN_PRODUCTS_TO_BTS_DTL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.215223+00:00'
    }
) }}

WITH 

source_st_cg1_x_mk_3c8r2alns_iface AS (
    SELECT
        pid_line_id,
        request_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        message_id,
        holding_unit_id,
        line_number,
        inventory_item_id,
        item_number,
        item_revision,
        product_quantity,
        return_reason_code,
        uom,
        ship_date,
        b2b_mfg_status,
        retry_count,
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
        ges_update_date,
        global_name,
        batch_id,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_cg1_x_mk_3c8r2alns_iface') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_cg1_x_mk_3c8r2alns_iface
)

SELECT * FROM final