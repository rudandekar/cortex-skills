{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_nsolt_core', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_NSOLT_CORE',
        'target_table': 'EX_WI_CG1_OE_ORDER_LINES_ALL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.413329+00:00'
    }
) }}

WITH 

source_st_cg_oe_order_lines_all AS (
    SELECT
        line_id,
        header_id,
        line_type_id,
        line_number,
        inventory_item_id,
        shipment_number,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        open_flag,
        booked_flag,
        org_id,
        ordered_item,
        request_date,
        promise_date,
        schedule_ship_date,
        ship_from_org_id,
        ship_to_org_id,
        invoice_to_org_id,
        deliver_to_org_id,
        ship_to_contact_id,
        deliver_to_contact_id,
        invoice_to_contact_id,
        intmed_ship_to_org_id,
        intmed_ship_to_contact_id,
        sold_from_org_id,
        sold_to_org_id,
        cust_po_number,
        earliest_acceptable_date,
        shippable_flag,
        ges_update_date,
        global_name,
        batch_id,
        action_cd,
        create_datetime,
        demand_class_code,
        derived_cust_req_ship_date
    FROM {{ source('raw', 'st_cg_oe_order_lines_all') }}
),

source_wi_cg1_oe_order_lines_all AS (
    SELECT
        sales_order_line_key,
        line_id,
        global_name,
        shippable_flag,
        earliest_acceptable_date,
        otm_ib_shipping_route_code
    FROM {{ source('raw', 'wi_cg1_oe_order_lines_all') }}
),

source_wi_nsolt_core AS (
    SELECT
        sales_order_line_key,
        sk_so_line_id_int,
        ss_code,
        bk_shipment_lot_number_int,
        bk_option_number_int,
        bk_so_line_number_int,
        bk_so_number_int,
        start_ssp_date,
        start_tv_dtm,
        end_ssp_date,
        end_tv_dtm,
        dispatch_dtm,
        print_queue_name,
        cntrct_conversion_status_name,
        dispatch_status_cd,
        sol_ship_to_cust_tax_exempt_cd,
        sol_taa_flg,
        init_prmsd_to_cust_shpmnt_dt
    FROM {{ source('raw', 'wi_nsolt_core') }}
),

source_n_sales_order_line_tv AS (
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
        sol_modified_by_erp_user_name
    FROM {{ source('raw', 'n_sales_order_line_tv') }}
),

final AS (
    SELECT
        line_id,
        global_name,
        shippable_flag,
        earliest_acceptable_date,
        exception_type
    FROM source_n_sales_order_line_tv
)

SELECT * FROM final