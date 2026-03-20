{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sol_ec_new_lines', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SOL_EC_NEW_LINES',
        'target_table': 'WI_SOL_EC_NEW_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.459864+00:00'
    }
) }}

WITH 

source_n_sol_end_customer AS (
    SELECT
        sales_order_line_key,
        end_customer_key,
        end_customer_type_code,
        ss_code,
        end_customer_assgn_level,
        edw_create_datetime,
        edw_create_user,
        edw_update_datetime,
        edw_update_user,
        dd_parent_sales_order_line_key
    FROM {{ source('raw', 'n_sol_end_customer') }}
),

source_n_so_end_customer AS (
    SELECT
        end_customer_key,
        dd_end_customer_type,
        sk_sales_order_header_id_int,
        ss_cd,
        sales_order_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_so_end_customer') }}
),

source_wi_sol_ec_new_lines_dist AS (
    SELECT
        ru_parnt_sls_order_line_key
    FROM {{ source('raw', 'wi_sol_ec_new_lines_dist') }}
),

source_wi_sol_ec_hdr_dist AS (
    SELECT
        sales_order_key
    FROM {{ source('raw', 'wi_sol_ec_hdr_dist') }}
),

source_wi_sol_ec_new_lines AS (
    SELECT
        sales_order_line_key,
        sales_order_key,
        ru_parnt_sls_order_line_key,
        sk_so_line_id_int,
        ss_code,
        start_tv_datetime,
        end_tv_datetime,
        edw_create_datetime
    FROM {{ source('raw', 'wi_sol_ec_new_lines') }}
),

source_wi_sol_ec_parent_lines AS (
    SELECT
        sales_order_line_key,
        end_customer_key,
        end_customer_type_code,
        ss_code,
        end_customer_assgn_level,
        edw_create_datetime,
        ru_parnt_sls_order_line_key
    FROM {{ source('raw', 'wi_sol_ec_parent_lines') }}
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
        bk_so_src_crt_by_user_id_int,
        bk_so_src_crt_datetime,
        bk_so_src_upd_by_user_id_int,
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
        complete_line_result_cd
    FROM {{ source('raw', 'n_sales_order_line_tv') }}
),

final AS (
    SELECT
        sales_order_line_key,
        sales_order_key,
        ru_parnt_sls_order_line_key,
        sk_so_line_id_int,
        ss_code,
        start_tv_datetime,
        end_tv_datetime,
        edw_create_datetime
    FROM source_n_sales_order_line_tv
)

SELECT * FROM final