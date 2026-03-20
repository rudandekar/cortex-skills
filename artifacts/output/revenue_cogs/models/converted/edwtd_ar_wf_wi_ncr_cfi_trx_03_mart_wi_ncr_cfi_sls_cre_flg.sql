{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ncr_cfi_trx', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_NCR_CFI_TRX',
        'target_table': 'WI_NCR_CFI_SLS_CRE_FLG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.714503+00:00'
    }
) }}

WITH 

source_n_direct_corporate_adjustment AS (
    SELECT
        direct_corp_adjustment_key,
        sales_order_line_key,
        dv_transaction_dt,
        transaction_dtm,
        adjusment_cogs_usd_amt,
        adjusment_cogs_functional_amt,
        adjustment_revenue_usd_amt,
        adjustment_revenue_functnl_amt,
        bk_direct_corp_adj_type_cd,
        direct_corp_adjustment_qty,
        transactional_currency_cd,
        bk_company_cd,
        set_of_books_key,
        sk_adjustment_id_int,
        ss_cd,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        sales_order_key
    FROM {{ source('raw', 'n_direct_corporate_adjustment') }}
),

source_wi_ncr_cfi_cur AS (
    SELECT
        direct_corp_adjustment_key,
        ss_cd,
        sales_order_key,
        sales_order_line_key,
        acctd_amount,
        usd_amount,
        transactional_currency_cd,
        transaction_dtm,
        functional_currency_code,
        line_id,
        amount,
        adjustment_id,
        fiscal_id,
        sk_organization_id_int,
        sk_inventory_item_id_int
    FROM {{ source('raw', 'wi_ncr_cfi_cur') }}
),

source_wi_ncr_cfi_sls_cre_flg AS (
    SELECT
        direct_corp_adjustment_key,
        source_type,
        line_id,
        metrix_flag,
        sales_credit_flag
    FROM {{ source('raw', 'wi_ncr_cfi_sls_cre_flg') }}
),

source_wi_ncr_cfi_cur AS (
    SELECT
        direct_corp_adjustment_key,
        ss_cd,
        sales_order_key,
        sales_order_line_key,
        acctd_amount,
        usd_amount,
        transactional_currency_cd,
        transaction_dtm,
        functional_currency_code,
        line_id,
        amount,
        adjustment_id,
        fiscal_id,
        sk_organization_id_int,
        sk_inventory_item_id_int
    FROM {{ source('raw', 'wi_ncr_cfi_cur') }}
),

source_el_sales_rep AS (
    SELECT
        sales_rep_id,
        global_name,
        sales_rep_number,
        sales_rep_name,
        sales_rep_status,
        sales_rep_number_int,
        attribute1,
        email_address,
        attribute3
    FROM {{ source('raw', 'el_sales_rep') }}
),

source_el_sales_territory AS (
    SELECT
        territory_id,
        global_name,
        hierarchy_type_code,
        territory_code,
        territory_name,
        territory_status,
        territory_description,
        territory_node_level1_code,
        creation_date,
        updated_date,
        sales_territory_key
    FROM {{ source('raw', 'el_sales_territory') }}
),

source_n_sales_order AS (
    SELECT
        sales_order_key,
        bk_company_code,
        bk_set_of_books_key,
        bk_so_number_int,
        start_tv_datetime,
        start_ssp_date,
        end_tv_datetime,
        end_ssp_date,
        shipment_priority_code,
        purchase_order_type_code,
        purchase_order_number,
        order_datetime,
        oracle_book_datetime,
        transactional_currency_code,
        invoicing_deferral_expir_dtm,
        early_ship_allowed_flag,
        partial_shipment_allowed_flag,
        sales_order_open_flag,
        customer_service_rep_name,
        auto_book_flag,
        last_updated_by_int,
        purchase_order_date,
        fax_received_datetime,
        complete_order_result_code,
        sales_order_expiration_dtm,
        cust_svc_rep_team_name,
        erp_sold_to_contact_id_int,
        erp_ship_to_contact_id_int,
        erp_bill_to_contact_id_int,
        conversion_type_code,
        original_system_reference_code,
        submitted_on_web_by_name,
        ru_cisco_booked_datetime,
        bk_inv_org_name_code,
        bk_deal_id,
        bk_price_list_name,
        bk_so_source_name,
        bk_order_type_name,
        bk_fob_point_code,
        bk_freight_terms_code,
        bk_so_src_crt_by_user_id_int,
        bk_so_src_crt_datetime,
        bk_so_src_upd_by_user_id_int,
        bk_so_src_upd_datetime,
        bk_sales_channel_code,
        bk_sales_channel_source_type,
        cisco_booked_status_role,
        sales_order_category_type,
        sk_sales_order_header_id_int,
        ss_code,
        cancelled_flag,
        pricing_datetime,
        ship_to_customer_key,
        bill_to_customer_key,
        sold_to_customer_key,
        ru_bk_orig_sales_order_key,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'n_sales_order') }}
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
        edw_create_user,
        edw_update_datetime
    FROM {{ source('raw', 'n_sales_order_line') }}
),

final AS (
    SELECT
        direct_corp_adjustment_key,
        source_type,
        line_id,
        metrix_flag,
        sales_credit_flag
    FROM source_n_sales_order_line
)

SELECT * FROM final