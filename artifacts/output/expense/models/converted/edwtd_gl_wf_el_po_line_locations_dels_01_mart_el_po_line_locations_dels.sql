{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_po_line_locations_dels', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_PO_LINE_LOCATIONS_DELS',
        'target_table': 'EL_PO_LINE_LOCATIONS_DELS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.645390+00:00'
    }
) }}

WITH 

source_st_mf_po_line_loc_all_dels AS (
    SELECT
        batch_id,
        accrue_on_receipt_flag,
        approved_date,
        approved_flag,
        attribute_category,
        attribute1,
        attribute11,
        attribute2,
        attribute3,
        attribute7,
        attribute9,
        cancel_date,
        cancel_flag,
        cancel_reason,
        cancelled_by,
        closed_by,
        closed_code,
        closed_date,
        closed_reason,
        created_by,
        creation_date,
        last_update_date,
        last_updated_by,
        line_location_id,
        match_option,
        need_by_date,
        note_to_receiver,
        org_id,
        po_header_id,
        po_line_id,
        promised_date,
        quantity,
        quantity_accepted,
        quantity_billed,
        quantity_cancelled,
        quantity_received,
        quantity_rejected,
        quantity_shipped,
        receipt_required_flag,
        ship_to_location_id,
        ship_to_organization_id,
        tax_code_id,
        taxable_flag,
        terms_id,
        ges_delete_date,
        ges_update_date,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_mf_po_line_loc_all_dels') }}
),

final AS (
    SELECT
        approved_date,
        approved_flag,
        creation_date,
        last_update_date,
        line_location_id,
        org_id,
        po_header_id,
        po_line_id,
        ges_delete_date,
        global_name
    FROM source_st_mf_po_line_loc_all_dels
)

SELECT * FROM final