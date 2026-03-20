{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_po_approved_supplier_list', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_PO_APPROVED_SUPPLIER_LIST',
        'target_table': 'FF_PO_APPROVED_SUPPLIER_LIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.005654+00:00'
    }
) }}

WITH 

source_po_approved_supplier_list AS (
    SELECT
        asl_id,
        using_organization_id,
        owning_organization_id,
        vendor_business_type,
        asl_status_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        manufacturer_id,
        vendor_id,
        item_id,
        category_id,
        vendor_site_id,
        primary_vendor_item,
        manufacturer_asl_id,
        review_by_date,
        comments,
        attribute_category,
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
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        disable_flag
    FROM {{ source('raw', 'po_approved_supplier_list') }}
),

final AS (
    SELECT
        using_organization_id,
        owning_organization_id,
        vendor_business_type,
        vendor_id,
        segment1,
        attribute2,
        attribute3,
        attribute4,
        disable_flag,
        vendor_site_id,
        name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date
    FROM source_po_approved_supplier_list
)

SELECT * FROM final