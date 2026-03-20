{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_vendor_invoice_line', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_VENDOR_INVOICE_LINE',
        'target_table': 'N_VENDOR_INVOICE_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.830832+00:00'
    }
) }}

WITH 

source_w_vendor_invoice_line AS (
    SELECT
        vendor_invoice_line_key,
        vendor_inv_line_cancelled_flg,
        source_deleted_flg,
        vendor_inv_line_qty,
        vendor_inv_line_trxl_amt,
        vendor_invoice_line_descr,
        bk_vendor_invoice_line_num_int,
        ss_cd,
        sk_invoice_id_int,
        sk_vendor_invoice_line_num_int,
        vendor_invoice_key,
        bk_vendor_inv_line_type_cd,
        bk_vendor_inv_line_src_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        receipt_shipment_line_key,
        purchase_order_line_key,
        reciving_transaction_key,
        posting_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_vendor_invoice_line') }}
),

final AS (
    SELECT
        vendor_invoice_line_key,
        vendor_inv_line_cancelled_flg,
        source_deleted_flg,
        vendor_inv_line_qty,
        vendor_inv_line_trxl_amt,
        vendor_invoice_line_descr,
        bk_vendor_invoice_line_num_int,
        ss_cd,
        sk_invoice_id_int,
        sk_vendor_invoice_line_num_int,
        vendor_invoice_key,
        bk_vendor_inv_line_type_cd,
        bk_vendor_inv_line_src_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        receipt_shipment_line_key,
        purchase_order_line_key,
        reciving_transaction_key,
        posting_dt,
        requestor_csco_wrkr_prty_key
    FROM source_w_vendor_invoice_line
)

SELECT * FROM final