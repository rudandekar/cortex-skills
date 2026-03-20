{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_service_contract', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SERVICE_CONTRACT',
        'target_table': 'WI_SERVICE_CONTRACT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.857302+00:00'
    }
) }}

WITH 

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

source_wi_service_contracts_int AS (
    SELECT
        sales_order_line_key,
        line_id,
        contract_number,
        start_date,
        end_date,
        edw_create_datetime,
        ss_code
    FROM {{ source('raw', 'wi_service_contracts_int') }}
),

source_sm_sales_order_line AS (
    SELECT
        sales_order_line_key,
        ss_code,
        sk_so_line_id_int,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_sales_order_line') }}
),

source_st_uo_xxcca_ser_cntrct_entl AS (
    SELECT
        batch_id,
        line_id,
        contract_number,
        start_date,
        end_date,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_uo_xxcca_ser_cntrct_entl') }}
),

source_st_bo_xxcca_ser_cntrct_entl AS (
    SELECT
        batch_id,
        line_id,
        contract_number,
        start_date,
        end_date,
        cdb_data_source,
        cdb_enqueue_time,
        cdb_dequeue_time,
        cdb_enqueue_seq,
        queue_id,
        process_set_id,
        q_creation_date,
        q_update_date,
        action_code,
        create_datetime
    FROM {{ source('raw', 'st_bo_xxcca_ser_cntrct_entl') }}
),

final AS (
    SELECT
        sales_order_line_key,
        srv_contract_number,
        srv_start_date,
        srv_end_date,
        ru_service_contract_number,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        line_id,
        item_category,
        revenue_type_code,
        edw_create_user,
        edw_create_datetime,
        ss_code
    FROM source_st_bo_xxcca_ser_cntrct_entl
)

SELECT * FROM final