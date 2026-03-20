{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_accts_pybl_vndr_pty_prf_splr', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_ACCTS_PYBL_VNDR_PTY_PRF_SPLR',
        'target_table': 'SM_ACCTS_PYBL_VNDRPTY_PRF_SPLR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.787264+00:00'
    }
) }}

WITH 

source_st_mf_po_apprd_supplier_lst AS (
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
    FROM {{ source('raw', 'st_mf_po_apprd_supplier_lst') }}
),

final AS (
    SELECT
        accts_pybl_vndrpty_prf_splrkey,
        owning_inventory_org_key,
        ap_vendor_party_key,
        bk_purchasing_item_category_id,
        edw_create_dtm,
        edw_create_user
    FROM source_st_mf_po_apprd_supplier_lst
)

SELECT * FROM final