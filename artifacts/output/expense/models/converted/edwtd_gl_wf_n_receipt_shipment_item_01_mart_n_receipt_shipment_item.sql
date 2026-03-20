{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_receipt_shipment_item', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_RECEIPT_SHIPMENT_ITEM',
        'target_table': 'N_RECEIPT_SHIPMENT_ITEM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.945058+00:00'
    }
) }}

WITH 

source_w_receipt_shipment_item AS (
    SELECT
        receipt_shipment_line_key,
        bk_shipment_line_number_int,
        actual_receipt_dtm,
        cancel_flg,
        line_num_int,
        note_txt,
        receipt_shipment_key,
        purchase_order_ln_shipment_key,
        dlvr_to_cisco_wrkr_party_key,
        receipt_shipment_ln_status_cd,
        source_deleted_flg,
        sk_shipment_line_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        primary_transaction_qty,
        trx_unit_prc_transactional_amt,
        transactional_currency_cd,
        override_conversion_rt,
        source_document_qty,
        primary_unit_of_measure_cd,
        src_doc_unit_of_measure_cd,
        rcpt_shipment_itm_creation_dtm,
        dv_rcpt_shipment_itm_create_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_receipt_shipment_item') }}
),

final AS (
    SELECT
        receipt_shipment_line_key,
        bk_shipment_line_number_int,
        actual_receipt_dtm,
        cancel_flg,
        line_num_int,
        note_txt,
        receipt_shipment_key,
        purchase_order_ln_shipment_key,
        dlvr_to_cisco_wrkr_party_key,
        receipt_shipment_ln_status_cd,
        source_deleted_flg,
        sk_shipment_line_id_int,
        ss_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        primary_transaction_qty,
        trx_unit_prc_transactional_amt,
        transactional_currency_cd,
        override_conversion_rt,
        source_document_qty,
        primary_unit_of_measure_cd,
        src_doc_unit_of_measure_cd,
        rcpt_shipment_itm_creation_dtm,
        dv_rcpt_shipment_itm_create_dt
    FROM source_w_receipt_shipment_item
)

SELECT * FROM final