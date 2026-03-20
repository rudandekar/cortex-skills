{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_order_line_v1_upd', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_ORDER_LINE_V1_UPD',
        'target_table': 'WI_SALES_ORDER_LINE_V1_UPD2',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.711648+00:00'
    }
) }}

WITH 

source_wi_sales_order_line_v1_upd1 AS (
    SELECT
        sales_order_line_key,
        bk_so_number_int,
        dd_top_model_sls_ordr_line_key,
        ru_config_prnt_sls_ordr_ln_key
    FROM {{ source('raw', 'wi_sales_order_line_v1_upd1') }}
),

source_n_sales_order_line_v1_tv AS (
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
        ru_config_sub_parent_sol_key
    FROM {{ source('raw', 'n_sales_order_line_v1_tv') }}
),

final AS (
    SELECT
        sales_order_line_key,
        ru_config_sub_parent_sol_key
    FROM source_n_sales_order_line_v1_tv
)

SELECT * FROM final