{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_xxscm_mk_crt_cnt_ib_iface', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_XXSCM_MK_CRT_CNT_IB_IFACE',
        'target_table': 'STG_CG1_XXSCM_MK_CRT_CNT_IB_IF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.673568+00:00'
    }
) }}

WITH 

source_stg_cg1_xxscm_mk_crt_cnt_ib_if AS (
    SELECT
        message_bom_pack_id,
        carton_content_id,
        carton_id,
        order_number,
        so_line_number,
        inventory_item_number,
        packout_quantity,
        parent_item_number,
        attribute1,
        attribute15,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag,
        last_update_date
    FROM {{ source('raw', 'stg_cg1_xxscm_mk_crt_cnt_ib_if') }}
),

source_cg1_xxscm_mk_crt_cnt_ib_iface AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        message_bom_pack_id,
        carton_content_id,
        carton_id,
        bom_ref_id,
        order_number,
        so_line_number,
        po_number,
        po_line_number,
        inventory_item_number,
        packout_quantity,
        uom,
        parent_item_number,
        country_of_origin,
        parent_serial_number,
        serial_number,
        rohs_code,
        at_signature,
        supplier,
        supplier_site,
        r12_po_receipt_status,
        sj_process_status,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        serial_num_status,
        attribute_context,
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
        decrypted_signature,
        decrypted_status
    FROM {{ source('raw', 'cg1_xxscm_mk_crt_cnt_ib_iface') }}
),

transformed_exp_cg1_xxscm_mk_crt_cnt_ib_iface AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    message_bom_pack_id,
    carton_content_id,
    carton_id,
    order_number,
    so_line_number,
    inventory_item_number,
    packout_quantity,
    parent_item_number,
    attribute1,
    attribute15,
    last_update_date
    FROM source_cg1_xxscm_mk_crt_cnt_ib_iface
),

final AS (
    SELECT
        message_bom_pack_id,
        carton_content_id,
        carton_id,
        order_number,
        so_line_number,
        inventory_item_number,
        packout_quantity,
        parent_item_number,
        attribute1,
        attribute15,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag,
        last_update_date
    FROM transformed_exp_cg1_xxscm_mk_crt_cnt_ib_iface
)

SELECT * FROM final