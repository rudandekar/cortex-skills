{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_po_approved_supplier_list', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_PO_APPROVED_SUPPLIER_LIST',
        'target_table': 'ST_PO_APPRD_SUPPLIER_LST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.733777+00:00'
    }
) }}

WITH 

source_ff_po_approved_supplier_list AS (
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
    FROM {{ source('raw', 'ff_po_approved_supplier_list') }}
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
    FROM source_ff_po_approved_supplier_list
)

SELECT * FROM final