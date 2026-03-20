{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_advncd_shpmnt_notice_po_line', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_ADVNCD_SHPMNT_NOTICE_PO_LINE',
        'target_table': 'N_ADVNCD_SHPMNT_NOTICE_PO_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.859247+00:00'
    }
) }}

WITH 

source_w_advncd_shpmnt_notice_po_line AS (
    SELECT
        asn_po_line_key,
        bk_purchase_order_line_key,
        bk_asn_received_dtm,
        dv_asn_received_dt,
        shipped_qty,
        asn_processed_status_cd,
        asn_processed_flg,
        sk_asn_line_id_int,
        product_key,
        receipt_shipment_key,
        src_rprtd_purchase_order_num,
        ep_container_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_advncd_shpmnt_notice_po_line') }}
),

final AS (
    SELECT
        asn_po_line_key,
        bk_purchase_order_line_key,
        bk_asn_received_dtm,
        dv_asn_received_dt,
        shipped_qty,
        asn_processed_status_cd,
        asn_processed_flg,
        sk_asn_line_id_int,
        product_key,
        receipt_shipment_key,
        src_rprtd_purchase_order_num,
        ep_container_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_advncd_shpmnt_notice_po_line
)

SELECT * FROM final