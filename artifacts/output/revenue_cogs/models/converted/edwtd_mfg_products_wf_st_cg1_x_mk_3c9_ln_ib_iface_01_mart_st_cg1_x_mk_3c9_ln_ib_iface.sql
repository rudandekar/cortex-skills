{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_x_mk_3c9_ln_ib_iface', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_X_MK_3C9_LN_IB_IFACE',
        'target_table': 'ST_CG1_X_MK_3C9_LN_IB_IFACE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.159442+00:00'
    }
) }}

WITH 

source_ff_cg1_x_mk_3c9_ln_ib_iface AS (
    SELECT
        message_id,
        transmission_date,
        holding_unit_id,
        po_line_number,
        item_number,
        item_revision,
        product_quantity,
        return_reason_code,
        return_instruction,
        uom,
        ship_date,
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
        attribute_context,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_cg1_x_mk_3c9_ln_ib_iface') }}
),

final AS (
    SELECT
        message_id,
        transmission_date,
        holding_unit_id,
        po_line_number,
        item_number,
        item_revision,
        product_quantity,
        return_reason_code,
        return_instruction,
        uom,
        ship_date,
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
        attribute_context,
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM source_ff_cg1_x_mk_3c9_ln_ib_iface
)

SELECT * FROM final