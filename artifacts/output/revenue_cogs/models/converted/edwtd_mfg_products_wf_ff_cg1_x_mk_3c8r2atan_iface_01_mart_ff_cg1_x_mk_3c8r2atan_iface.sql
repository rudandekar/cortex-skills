{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cg1_x_mk_3c8r2atan_iface', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_FF_CG1_X_MK_3C8R2ATAN_IFACE',
        'target_table': 'FF_CG1_X_MK_3C8R2ATAN_IFACE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.389624+00:00'
    }
) }}

WITH 

source_cg1_x_mk_3c8r2atan_iface AS (
    SELECT
        tan_id,
        request_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        message_id,
        pid_line_id,
        tan_number,
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
        global_name
    FROM {{ source('raw', 'cg1_x_mk_3c8r2atan_iface') }}
),

transformed_exp_ff_cg1_x_mk_3c8r2atan_iface AS (
    SELECT
    tan_id,
    request_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    message_id,
    pid_line_id,
    tan_number,
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
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_create_datetime,
    'I' AS o_action_code
    FROM source_cg1_x_mk_3c8r2atan_iface
),

final AS (
    SELECT
        tan_id,
        request_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        message_id,
        pid_line_id,
        tan_number,
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
    FROM transformed_exp_ff_cg1_x_mk_3c8r2atan_iface
)

SELECT * FROM final