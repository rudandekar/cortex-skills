{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rtrn_itms_shpmt_rcpt_cnfrmtn', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_RTRN_ITMS_SHPMT_RCPT_CNFRMTN',
        'target_table': 'EX_CG1_X_MK_3C9_LN_IB_IFACE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.471985+00:00'
    }
) }}

WITH 

source_st_cg1_x_mk_3c9_ln_ib_iface AS (
    SELECT
        global_name,
        transmission_date,
        ship_date,
        creation_date,
        last_update_date,
        ges_update_date,
        request_id,
        created_by,
        last_updated_by,
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
        message_id,
        item_revision,
        uom,
        attribute_context,
        holding_unit_id,
        po_line_number,
        item_number,
        product_quantity,
        return_reason_code,
        return_instruction,
        process_status,
        batch_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_cg1_x_mk_3c9_ln_ib_iface') }}
),

final AS (
    SELECT
        global_name,
        transmission_date,
        ship_date,
        creation_date,
        last_update_date,
        ges_update_date,
        request_id,
        created_by,
        last_updated_by,
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
        message_id,
        item_revision,
        uom,
        attribute_context,
        holding_unit_id,
        po_line_number,
        item_number,
        product_quantity,
        return_reason_code,
        return_instruction,
        process_status,
        batch_id,
        create_datetime,
        action_code,
        exception_type
    FROM source_st_cg1_x_mk_3c9_ln_ib_iface
)

SELECT * FROM final