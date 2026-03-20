{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_po_shpmnt_dstrbtn_revision', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_PO_SHPMNT_DSTRBTN_REVISION',
        'target_table': 'N_PO_SHPMNT_DSTRBTN_REVISION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.897172+00:00'
    }
) }}

WITH 

source_w_po_shpmnt_dstrbtn_revision AS (
    SELECT
        po_shipment_distribution_key,
        bk_po_revision_num_int,
        purchase_order_key,
        deliver_to_cisco_wrkr_pty_key,
        purchase_reqn_line_distrib_key,
        current_record_flg,
        general_ledger_account_key,
        pd_company_cd,
        pd_project_cd,
        pd_financial_loc_cd,
        pd_dept_cd,
        pd_financial_acct_cd,
        pd_subacct_cd,
        pd_po_line_shipment_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_po_shpmnt_dstrbtn_revision') }}
),

final AS (
    SELECT
        po_shipment_distribution_key,
        bk_po_revision_num_int,
        purchase_order_key,
        deliver_to_cisco_wrkr_pty_key,
        purchase_reqn_line_distrib_key,
        current_record_flg,
        general_ledger_account_key,
        pd_company_cd,
        pd_project_cd,
        pd_financial_loc_cd,
        pd_dept_cd,
        pd_financial_acct_cd,
        pd_subacct_cd,
        pd_po_line_shipment_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_po_shpmnt_dstrbtn_revision
)

SELECT * FROM final