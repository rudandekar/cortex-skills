{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_as_built_bom', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_MT_AS_BUILT_BOM',
        'target_table': 'WI_AS_BUILT_BOM_SOL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.267604+00:00'
    }
) }}

WITH 

source_wi_as_built_bom_vil AS (
    SELECT
        bk_purchase_order_line_num_int,
        purchase_order_line_key,
        po_line_ordered_qty,
        bk_purchase_order_create_dt,
        actual_receipt_dtm,
        bk_vendor_invoice_num,
        vendor_invoice_key,
        vendor_invoice_line_key,
        vendor_inv_line_qty,
        vendor_inv_line_trxl_amt,
        bk_inventory_org_name_code,
        inventory_orgn_name_key,
        bk_so_number_int,
        sales_order_line_key,
        bk_so_line_number_int,
        product_key
    FROM {{ source('raw', 'wi_as_built_bom_vil') }}
),

source_n_sales_order_line AS (
    SELECT
        sales_order_line_key,
        bk_company_code,
        bk_set_of_books_key,
        bk_so_number_int,
        bk_so_line_number_int,
        bk_shipment_lot_number_int,
        bk_option_number_int,
        bk_ship_from_inv_org_name_code,
        start_tv_datetime,
        start_ssp_date,
        end_tv_datetime,
        end_ssp_date,
        bookings_percent,
        order_quantity,
        unit_sale_price,
        unit_list_price,
        unit_trade_in_usd_amount,
        revenue_type_code,
        item_type_code,
        cisco_booked_datetime,
        cisco_booked_status_code,
        shipped_quantity,
        so_line_closed_date,
        erp_ship_to_contact_id_int,
        so_line_open_flag,
        scheduled_ship_date,
        cust_requested_shipment_date,
        promised_to_cust_shipment_date,
        cust_shipment_recommit_date,
        ship_set_number_int,
        invoiced_quantity,
        shipment_confirmed_result_code,
        shipment_confirmed_date,
        factory_recommit_date,
        freight_carrier_code,
        pick_release_date,
        pick_release_result_code,
        cisco_schedule_result_code,
        cisco_schedule_result_date,
        so_line_option_flag,
        ar_interface_result_code,
        ar_interface_datetime,
        ru_sol_return_context_code,
        ru_sol_return_reason_code,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        ru_service_contract_number,
        bk_so_src_crt_datetime,
        bk_so_src_upd_datetime,
        service_contract_role,
        major_line_type,
        sales_order_category_type,
        sk_so_line_id_int,
        edw_create_user,
        ss_code,
        cancelled_flag,
        item_category_description,
        pricing_datetime,
        product_key,
        sales_order_key,
        ship_to_customer_key,
        ru_parnt_sls_order_line_key,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        advanced_services_project_cd,
        ru_bundle_sales_order_line_key,
        line_status_cd,
        complete_line_result_cd,
        sol_created_by_erp_user_name,
        sol_modified_by_erp_user_name,
        dd_shipment_confirmed_dtm,
        cisco_scheduled_result_dtm,
        pick_release_dtm,
        dv_sol_cancelled_qty,
        dd_parent_line_product_key,
        dv_option_class_line_flg,
        dv_service_category_cd
    FROM {{ source('raw', 'n_sales_order_line') }}
),

source_wi_as_built_bom_sol AS (
    SELECT
        sales_order_line_key,
        bk_ship_from_inv_org_name_code,
        shipped_quantity,
        bk_so_number_int,
        bk_so_line_number_int,
        item_type_code,
        scheduled_ship_date,
        product_key,
        actual_receipt_dtm
    FROM {{ source('raw', 'wi_as_built_bom_sol') }}
),

final AS (
    SELECT
        sales_order_line_key,
        bk_ship_from_inv_org_name_code,
        shipped_quantity,
        bk_so_number_int,
        bk_so_line_number_int,
        item_type_code,
        scheduled_ship_date,
        product_key,
        actual_receipt_dtm
    FROM source_wi_as_built_bom_sol
)

SELECT * FROM final