{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_advncd_shpmnt_notice_po_line', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_W_ADVNCD_SHPMNT_NOTICE_PO_LINE',
        'target_table': 'W_ADVNCD_SHPMNT_NOTICE_PO_LINE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.404857+00:00'
    }
) }}

WITH 

source_st_cg1_xxscm_mk_asnline_ib AS (
    SELECT
        asn_line_id,
        ship_set_number,
        inventory_item_number,
        shipped_quantity,
        uom,
        serial_number,
        asn_processed_flag,
        asn_processed_status,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        serial_num_status,
        message_3b2_id,
        container_id,
        po_number,
        po_release_line_number,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_cg1_xxscm_mk_asnline_ib') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_cg1_xxscm_mk_asnline_ib
)

SELECT * FROM final