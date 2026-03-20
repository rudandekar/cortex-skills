{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_return_products_to_bts_srl', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_RETURN_PRODUCTS_TO_BTS_SRL',
        'target_table': 'W_RETURN_PRODUCTS_TO_BTS_SRL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.740829+00:00'
    }
) }}

WITH 

source_st_cg1_x_mk_3c8r2asrn_iface AS (
    SELECT
        serial_id,
        request_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        message_id,
        pid_line_id,
        serial_no,
        inventory_item_id,
        item_integer,
        item_revision,
        quantity,
        serial_status,
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
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_x_mk_3c8r2asrn_iface') }}
),

final AS (
    SELECT
        bk_serial_num,
        process_status_cd,
        create_dtm,
        dv_create_dt,
        sk_serial_id_int,
        sk_pid_line_id_int,
        sk_message_id,
        return_products_to_bts_dtl_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_cg1_x_mk_3c8r2asrn_iface
)

SELECT * FROM final