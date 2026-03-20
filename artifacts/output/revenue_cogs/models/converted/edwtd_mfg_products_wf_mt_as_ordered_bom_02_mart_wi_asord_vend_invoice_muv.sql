{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_as_ordered_bom', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_MT_AS_ORDERED_BOM',
        'target_table': 'WI_ASORD_VEND_INVOICE_MUV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.494072+00:00'
    }
) }}

WITH 

source_wi_as_ordered_bom_muv AS (
    SELECT
        sales_order_line_key,
        bk_so_number_int,
        bk_so_line_number_int,
        pid_item_key,
        inventory_orgn_name_key,
        major_pid,
        bk_product_id,
        major_item_key,
        order_quantity,
        bk_so_src_crt_datetime,
        item_type_code,
        item_wip_supply_type_cd,
        scheduled_ship_date,
        bk_inventory_org_name_code,
        bk_purchase_order_num,
        bk_purchase_order_line_num_int,
        purchase_order_line_key,
        actual_receipt_dtm
    FROM {{ source('raw', 'wi_as_ordered_bom_muv') }}
),

source_wi_asord_vend_invoice_muv AS (
    SELECT
        bk_vendor_invoice_num,
        vendor_invoice_key,
        vendor_invoice_line_key,
        vendor_inv_line_qty,
        vendor_inv_line_trxl_amt,
        bk_purchase_order_num,
        purchase_order_line_key
    FROM {{ source('raw', 'wi_asord_vend_invoice_muv') }}
),

source_mt_as_ordered_bom AS (
    SELECT
        bk_so_number_int,
        sales_order_line_key,
        bk_product_id,
        pid_item_key,
        bk_inventory_org_name_code,
        bk_purchase_order_num,
        bk_purchase_order_line_num_int,
        purchase_order_line_key,
        flb_id,
        flb_item_key,
        bom_wip_supply_type_cd,
        order_quantity,
        bk_so_src_crt_datetime,
        bom_sales_order_qty,
        sales_order_std_cost_amt,
        sales_order_ext_amt,
        actual_receipt_dtm,
        bk_vendor_invoice_num,
        vendor_invoice_key,
        vendor_invoice_line_key,
        vendor_inv_line_qty,
        vendor_inv_line_trxl_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_as_ordered_bom') }}
),

final AS (
    SELECT
        bk_vendor_invoice_num,
        vendor_invoice_key,
        vendor_invoice_line_key,
        vendor_inv_line_qty,
        vendor_inv_line_trxl_amt,
        bk_purchase_order_num,
        purchase_order_line_key
    FROM source_mt_as_ordered_bom
)

SELECT * FROM final