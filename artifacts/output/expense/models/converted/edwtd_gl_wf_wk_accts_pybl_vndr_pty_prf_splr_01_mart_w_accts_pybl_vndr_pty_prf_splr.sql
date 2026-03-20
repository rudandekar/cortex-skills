{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_accts_pybl_vndr_pty_prf_splr', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WK_ACCTS_PYBL_VNDR_PTY_PRF_SPLR',
        'target_table': 'W_ACCTS_PYBL_VNDR_PTY_PRF_SPLR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.726279+00:00'
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
        using_inventory_org_key,
        ap_vendor_party_key,
        bk_purchasing_item_category_id,
        owning_inventory_org_key,
        vendor_business_type_cd,
        supplier_status_cd,
        supplier_segment_status_cd,
        parent_supplier_name,
        disabled_flg,
        pref_supplier_vendor_prty_key,
        bk_operating_unit_name_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_mf_po_apprd_supplier_lst
)

SELECT * FROM final