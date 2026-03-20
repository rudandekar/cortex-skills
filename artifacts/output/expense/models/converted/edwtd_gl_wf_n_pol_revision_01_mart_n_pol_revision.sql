{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pol_revision', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_POL_REVISION',
        'target_table': 'N_POL_REVISION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.991564+00:00'
    }
) }}

WITH 

source_w_pol_revision AS (
    SELECT
        purchase_order_line_key,
        bk_po_revision_num_int,
        purchase_order_key,
        po_line_ordered_qty,
        po_line_unit_price_trxl_amt,
        dv_incrmntl_change_trxl_amt,
        po_line_status_cd,
        po_line_item_desc,
        current_record_flg,
        service_role,
        ru_service_start_dt,
        ru_service_end_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_pol_revision') }}
),

final AS (
    SELECT
        purchase_order_line_key,
        bk_po_revision_num_int,
        purchase_order_key,
        po_line_ordered_qty,
        po_line_unit_price_trxl_amt,
        dv_incrmntl_change_trxl_amt,
        po_line_status_cd,
        po_line_item_desc,
        current_record_flg,
        service_role,
        ru_service_start_dt,
        ru_service_end_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_pol_revision
)

SELECT * FROM final