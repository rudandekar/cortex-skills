{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_po_revision', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_PO_REVISION',
        'target_table': 'N_PO_REVISION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.013730+00:00'
    }
) }}

WITH 

source_w_po_revision AS (
    SELECT
        purchase_order_key,
        bk_po_revision_num_int,
        last_revised_dt,
        vendor_pty_key,
        buyer_cisco_worker_pty_key,
        purchase_order_create_dt,
        current_record_flg,
        dv_fiscal_year_mth_num_int,
        receiving_location_locator_key,
        approval_dtm,
        dv_approval_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_po_revision') }}
),

final AS (
    SELECT
        purchase_order_key,
        bk_po_revision_num_int,
        last_revised_dt,
        vendor_pty_key,
        buyer_cisco_worker_pty_key,
        purchase_order_create_dt,
        current_record_flg,
        dv_fiscal_year_mth_num_int,
        receiving_location_locator_key,
        approval_dtm,
        dv_approval_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_po_revision
)

SELECT * FROM final