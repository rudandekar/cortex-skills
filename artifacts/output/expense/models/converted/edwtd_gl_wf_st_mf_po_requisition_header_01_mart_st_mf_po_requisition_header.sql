{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_po_requisition_header', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_PO_REQUISITION_HEADER',
        'target_table': 'ST_MF_PO_REQUISITION_HEADER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.139033+00:00'
    }
) }}

WITH 

source_ff_mf_po_requisition_header AS (
    SELECT
        batch_id,
        apps_source_code,
        attribute1,
        attribute10,
        attribute11,
        attribute2,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        authorization_status,
        cancel_flag,
        change_pending_flag,
        closed_code,
        created_by,
        creation_date,
        description,
        enabled_flag,
        interface_source_code,
        interface_source_line_id,
        last_update_date,
        last_updated_by,
        note_to_authorizer,
        org_id,
        preparer_id,
        program_update_date,
        request_id,
        requisition_header_id,
        segment1,
        summary_flag,
        transferred_to_oe_flag,
        type_lookup_code,
        ges_update_date,
        global_name,
        create_datetime,
        action_code,
        attribute12
    FROM {{ source('raw', 'ff_mf_po_requisition_header') }}
),

final AS (
    SELECT
        batch_id,
        apps_source_code,
        attribute1,
        attribute10,
        attribute11,
        attribute2,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        authorization_status,
        cancel_flag,
        change_pending_flag,
        closed_code,
        created_by,
        creation_date,
        description,
        enabled_flag,
        interface_source_code,
        interface_source_line_id,
        last_update_date,
        last_updated_by,
        note_to_authorizer,
        org_id,
        preparer_id,
        program_update_date,
        request_id,
        requisition_header_id,
        segment1,
        summary_flag,
        transferred_to_oe_flag,
        type_lookup_code,
        ges_update_date,
        global_name,
        create_datetime,
        action_code,
        attribute12
    FROM source_ff_mf_po_requisition_header
)

SELECT * FROM final