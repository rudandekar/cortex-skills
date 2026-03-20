{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_purchase_requisition_line', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_PURCHASE_REQUISITION_LINE',
        'target_table': 'N_PURCHASE_REQUISITION_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.778799+00:00'
    }
) }}

WITH 

source_w_purchase_requisition_line AS (
    SELECT
        purchase_requisition_line_key,
        bk_purchase_requisition_num,
        bk_pr_line_number_int,
        project_locality_int,
        functional_project_cd,
        transactional_currency_cd,
        pr_line_item_descr,
        dv_pr_line_usd_amt,
        pr_line_transactional_amt,
        pr_line_functional_amt,
        pr_line_catalog_flg,
        requestor_cisco_wrkr_party_key,
        itm_cat_mgr_csco_wrkr_prty_key,
        ap_vendor_site_party_key,
        ap_vendor_party_key,
        source_deleted_flg,
        rptd_dlvr_csco_wrkr_prty_name,
        bk_purchasing_item_category_id,
        sk_pr_line_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type,
        catalog_type_cd,
        item_catalog_source_id_int
    FROM {{ source('raw', 'w_purchase_requisition_line') }}
),

final AS (
    SELECT
        purchase_requisition_line_key,
        bk_purchase_requisition_num,
        bk_pr_line_number_int,
        project_locality_int,
        functional_project_cd,
        transactional_currency_cd,
        pr_line_item_descr,
        dv_pr_line_usd_amt,
        pr_line_transactional_amt,
        pr_line_functional_amt,
        pr_line_catalog_flg,
        requestor_cisco_wrkr_party_key,
        itm_cat_mgr_csco_wrkr_prty_key,
        ap_vendor_site_party_key,
        ap_vendor_party_key,
        source_deleted_flg,
        rptd_dlvr_csco_wrkr_prty_name,
        bk_purchasing_item_category_id,
        sk_pr_line_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        catalog_type_cd,
        item_catalog_source_id_int,
        prepayment_flg
    FROM source_w_purchase_requisition_line
)

SELECT * FROM final