{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_purchase_order_ln_shipment', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_PURCHASE_ORDER_LN_SHIPMENT',
        'target_table': 'N_PURCHASE_ORDER_LN_SHIPMENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.598808+00:00'
    }
) }}

WITH 

source_w_purchase_order_ln_shipment AS (
    SELECT
        purchase_order_ln_shipment_key,
        bk_po_line_shipment_number_int,
        bk_purchase_order_num,
        bk_purchase_order_create_dt,
        bk_purchase_order_line_num_int,
        purchase_order_line_key,
        po_line_shipment_need_dt,
        po_line_shipment_promised_dt,
        po_line_shipment_status_cd,
        po_line_shipment_cancel_flg,
        po_line_shipment_last_accpt_dt,
        po_line_shpmt_prc_override_amt,
        po_line_receipt_required_flg,
        po_inspection_required_flg,
        purchase_requisition_line_key,
        source_deleted_flg,
        po_line_shipment_accepted_qty,
        po_line_shipment_received_qty,
        po_line_shipment_rejected_qty,
        po_line_shipment_billed_qty,
        dd_purchase_order_key,
        sk_po_line_location_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        service_location_descr,
        ship_to_inventory_org_key,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_purchase_order_ln_shipment') }}
),

final AS (
    SELECT
        purchase_order_ln_shipment_key,
        bk_po_line_shipment_number_int,
        bk_purchase_order_num,
        bk_purchase_order_create_dt,
        bk_purchase_order_line_num_int,
        purchase_order_line_key,
        po_line_shipment_need_dt,
        po_line_shipment_promised_dt,
        po_line_shipment_status_cd,
        po_line_shipment_cancel_flg,
        po_line_shipment_last_accpt_dt,
        po_line_shpmt_prc_override_amt,
        po_line_receipt_required_flg,
        po_inspection_required_flg,
        purchase_requisition_line_key,
        source_deleted_flg,
        po_line_shipment_accepted_qty,
        po_line_shipment_received_qty,
        po_line_shipment_rejected_qty,
        po_line_shipment_billed_qty,
        dd_purchase_order_key,
        sk_po_line_location_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        service_location_descr,
        ship_to_inventory_org_key
    FROM source_w_purchase_order_ln_shipment
)

SELECT * FROM final