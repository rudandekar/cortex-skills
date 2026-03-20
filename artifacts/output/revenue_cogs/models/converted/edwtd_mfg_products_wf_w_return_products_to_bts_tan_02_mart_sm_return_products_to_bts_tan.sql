{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_return_products_to_bts_tan', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_RETURN_PRODUCTS_TO_BTS_TAN',
        'target_table': 'SM_RETURN_PRODUCTS_TO_BTS_TAN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.747923+00:00'
    }
) }}

WITH 

source_st_cg1_x_mk_3c8r2atan_iface AS (
    SELECT
        tan_id,
        request_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        message_id,
        pid_line_id,
        tan_integer,
        tan_quantity,
        inventory_item_id,
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
    FROM {{ source('raw', 'st_cg1_x_mk_3c8r2atan_iface') }}
),

final AS (
    SELECT
        return_products_to_bts_tan_key,
        sk_message_id,
        sk_pid_line_id_int,
        sk_tan_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_st_cg1_x_mk_3c8r2atan_iface
)

SELECT * FROM final