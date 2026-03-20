{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_retro_payment_cost_exception', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_N_RETRO_PAYMENT_COST_EXCEPTION',
        'target_table': 'N_RETRO_PAYMENT_COST_EXCEPTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.041162+00:00'
    }
) }}

WITH 

source_w_retro_payment_cost_exception AS (
    SELECT
        bk_exception_id_int,
        flow_type,
        df_or_flb_type,
        retro_invoice_status_cd,
        retro_usd_amt,
        shipped_qty,
        last_update_dtm,
        inventory_item_usd_cost,
        original_exception_type_cd,
        exception_type_cd,
        exception_status_cd,
        creation_dtm,
        major_product_key,
        inventory_orgn_name_key,
        purchase_order_line_key,
        vendor_invoice_line_key,
        retro_invoice_line_key,
        ru_shipment_dtm,
        ru_shipment_num,
        ru_received_dtm,
        ru_resolved_flb_usd_cost,
        ru_flb_qty,
        ru_flb_inventory_org_key,
        ru_flb_item_key,
        ru_drct_fulfilmnt_fee_usd_cost,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_resolution_dt,
        resolution_dtm,
        reason_cd,
        initial_reason_cd,
        receipt_num,
        rslvd_drct_fulflmt_fee_usd_amt,
        advnc_shpmt_notice_create_dt,
        ru_fwd_ref_exception_id_int,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_retro_payment_cost_exception') }}
),

final AS (
    SELECT
        bk_exception_id_int,
        flow_type,
        df_or_flb_type,
        retro_invoice_status_cd,
        retro_usd_amt,
        shipped_qty,
        last_update_dtm,
        inventory_item_usd_cost,
        original_exception_type_cd,
        exception_type_cd,
        exception_status_cd,
        creation_dtm,
        major_product_key,
        inventory_orgn_name_key,
        purchase_order_line_key,
        vendor_invoice_line_key,
        retro_invoice_line_key,
        ru_shipment_dtm,
        ru_shipment_num,
        ru_received_dtm,
        ru_resolved_flb_usd_cost,
        ru_flb_qty,
        ru_flb_inventory_org_key,
        ru_flb_item_key,
        ru_drct_fulfilmnt_fee_usd_cost,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_resolution_dt,
        resolution_dtm,
        reason_cd,
        initial_reason_cd,
        receipt_num,
        rslvd_drct_fulflmt_fee_usd_amt,
        advnc_shpmt_notice_create_dt,
        ru_fwd_ref_exception_id_int
    FROM source_w_retro_payment_cost_exception
)

SELECT * FROM final