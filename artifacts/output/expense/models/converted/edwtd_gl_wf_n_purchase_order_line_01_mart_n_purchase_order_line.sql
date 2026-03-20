{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_purchase_order_line', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_PURCHASE_ORDER_LINE',
        'target_table': 'N_PURCHASE_ORDER_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.037445+00:00'
    }
) }}

WITH 

source_w_purchase_order_line AS (
    SELECT
        purchase_order_line_key,
        bk_purchase_order_line_type_cd,
        bk_purchase_order_num,
        bk_purchase_order_create_dt,
        po_line_item_descr,
        po_line_note_txt,
        po_line_unit_price_amt,
        po_line_cancelled_role,
        po_line_ordered_qty,
        bk_purchase_order_line_num_int,
        po_line_status_cd,
        source_deleted_flg,
        purchase_order_key,
        item_unit_of_measure_cd,
        ru_po_line_cancelled_dt,
        ru_po_line_cancelled_qty,
        ru_po_ln_cncld_wrkr_prty_key,
        ru_po_line_cncld_reason_descr,
        sk_po_line_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        po_line_taxable_flg,
        ru_service_start_dt,
        ru_service_end_dt,
        product_key,
        bk_original_dept_cd,
        bk_original_company_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_purchase_order_line') }}
),

final AS (
    SELECT
        purchase_order_line_key,
        bk_purchase_order_line_type_cd,
        bk_purchase_order_num,
        bk_purchase_order_create_dt,
        po_line_item_descr,
        po_line_note_txt,
        po_line_unit_price_amt,
        po_line_cancelled_role,
        po_line_ordered_qty,
        bk_purchase_order_line_num_int,
        po_line_status_cd,
        source_deleted_flg,
        purchase_order_key,
        item_unit_of_measure_cd,
        ru_po_line_cancelled_dt,
        ru_po_line_cancelled_qty,
        ru_po_ln_cncld_wrkr_prty_key,
        ru_po_line_cncld_reason_descr,
        sk_po_line_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        po_line_taxable_flg,
        ru_service_start_dt,
        ru_service_end_dt,
        cworker_sow_id,
        product_key,
        bk_original_dept_cd,
        bk_original_company_cd
    FROM source_w_purchase_order_line
)

SELECT * FROM final