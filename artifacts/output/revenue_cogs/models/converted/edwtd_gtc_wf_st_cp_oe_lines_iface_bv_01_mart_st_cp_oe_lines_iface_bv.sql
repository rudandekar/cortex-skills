{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cp_oe_lines_iface_bv', 'batch', 'edwtd_gtc'],
    meta={
        'source_workflow': 'wf_m_ST_CP_OE_LINES_IFACE_BV',
        'target_table': 'ST_CP_OE_LINES_IFACE_BV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.553137+00:00'
    }
) }}

WITH 

source_bv_cp_oe_lines_iface AS (
    SELECT
        vt_interface_id,
        vt_transaction_type,
        vt_transaction_ref,
        vt_transaction_date,
        vt_date_processed,
        inventory_item,
        unit_selling_price,
        attribute1,
        attribute2,
        attribute4,
        attribute5,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute13,
        attribute15,
        attribute17,
        attribute20,
        attribute21,
        attribute24,
        attribute26,
        attribute29,
        attribute33,
        vt_transaction_id,
        vt_source_assignment_id,
        line_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag,
        attribute37,
        attribute40,
        org_id,
        attribute12
    FROM {{ source('raw', 'bv_cp_oe_lines_iface') }}
),

final AS (
    SELECT
        vt_interface_id,
        vt_transaction_type,
        vt_transaction_ref,
        vt_transaction_date,
        vt_date_processed,
        inventory_item,
        unit_selling_price,
        attribute1,
        attribute2,
        attribute4,
        attribute5,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute13,
        attribute15,
        attribute17,
        attribute20,
        attribute21,
        attribute24,
        attribute26,
        attribute29,
        attribute33,
        vt_transaction_id,
        vt_source_assignment_id,
        line_id,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        attribute37,
        attribute40,
        org_id,
        attribute12
    FROM source_bv_cp_oe_lines_iface
)

SELECT * FROM final