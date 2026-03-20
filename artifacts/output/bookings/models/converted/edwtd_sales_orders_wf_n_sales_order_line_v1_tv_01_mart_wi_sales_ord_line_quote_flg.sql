{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sales_order_line_v1_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SALES_ORDER_LINE_V1_TV',
        'target_table': 'WI_SALES_ORD_LINE_QUOTE_FLG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.521101+00:00'
    }
) }}

WITH 

source_w_sales_order_line_v1 AS (
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_service_quote_num,
        as_reported_service_quote_num,
        dv_dup_service_quote_num_flg,
        goods_prod_sls_order_line_key,
        assemble_to_sls_ord_line_key,
        split_from_sls_order_line_key,
        accts_rcvbl_interface_dtm,
        accts_rcvbl_interface_stat_cd,
        sol_ship_method_id,
        bundle_goods_product_key,
        constituent_goods_product_key,
        sol_ship_to_cust_tax_exempt_cd,
        delivery_option_cd,
        cust_trx_reference_num,
        src_rptd_ship_to_email_cmt,
        dv_bk_so_src_crt_dt,
        sol_taa_flg,
        dd_top_model_sls_ordr_line_key,
        ru_config_prnt_sls_ordr_ln_key,
        product_config_checked_flg,
        ru_config_sub_parent_sol_key,
        early_build_role,
        ru_early_build_start_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_order_line_v1') }}
),

source_w_sales_order_line_v1 AS (
    SELECT
        sales_order_line_key,
        newfield,
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_service_quote_num,
        as_reported_service_quote_num,
        dv_dup_service_quote_num_flg,
        goods_prod_sls_order_line_key,
        assemble_to_sls_ord_line_key,
        split_from_sls_order_line_key,
        accts_rcvbl_interface_dtm,
        accts_rcvbl_interface_stat_cd,
        sol_ship_method_id,
        bundle_goods_product_key,
        constituent_goods_product_key,
        sol_ship_to_cust_tax_exempt_cd,
        delivery_option_cd,
        cust_trx_reference_num,
        src_rptd_ship_to_email_cmt,
        dv_bk_so_src_crt_dt,
        sol_taa_flg,
        dd_top_model_sls_ordr_line_key,
        ru_config_prnt_sls_ordr_ln_key,
        product_config_checked_flg,
        ru_config_sub_parent_sol_key,
        early_build_role,
        ru_early_build_start_dt,
        contract_duration_role,
        ru_srvc_cntrct_drtn_mnths_cnt,
        earliest_acceptable_dlvry_dt,
        shipping_route_code,
        shippable_role,
        svc_contract_effect_days_cnt,
        default_freight_terms_cd,
        default_frght_pymnt_method_cd,
        default_freight_on_board_cd,
        invoice_eligibility_event_cd,
        init_cisco_schdl_result_dtm,
        dv_init_cisco_schdl_result_dt,
        init_prmsd_to_cust_shpmnt_dt,
        so_line_type_name,
        bk_order_qty_unit_measure_cd,
        init_scheduled_ship_dt,
        om_exception_role,
        just_in_time_start_dt,
        demand_class_cd,
        originated_qtc_via_cg1_flg,
        legacy_shipment_priority_cd,
        reseller_bill_to_cust_key,
        pricing_qty,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sales_order_line_v1') }}
),

source_ex_sales_ord_line_quote AS (
    SELECT
        as_reported_service_quote_num,
        create_dtm,
        update_dtm
    FROM {{ source('raw', 'ex_sales_ord_line_quote') }}
),

source_wi_sales_ord_line_quote_flg AS (
    SELECT
        sales_order_line_key,
        bk_service_quote_num,
        sol_dv_quote_num_flg,
        rank_index,
        dv_dup_service_quote_num_flg,
        edw_update_dtm
    FROM {{ source('raw', 'wi_sales_ord_line_quote_flg') }}
),

final AS (
    SELECT
        sales_order_line_key,
        bk_service_quote_num,
        sol_dv_quote_num_flg,
        rank_index,
        dv_dup_service_quote_num_flg,
        edw_update_dtm
    FROM source_wi_sales_ord_line_quote_flg
)

SELECT * FROM final