{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_so_ncb_tv_xaas', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_N_SO_NCB_TV_XAAS',
        'target_table': 'N_SO_NCB_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.884396+00:00'
    }
) }}

WITH 

source_n_so_ncb_tv AS (
    SELECT
        sales_order_key,
        start_tv_dt,
        end_tv_dt,
        dv_ncb_interntnl_demo_flg,
        dv_ncb_replacement_demo_flg,
        dv_ncb_revenue_flg,
        dv_ncb_acquisition_flg,
        dv_ncb_ic_revenue_flg,
        dd_ncb_bill_to_customer_key,
        dd_ncb_oracle_book_dtm,
        dd_ncb_cst_srvc_rep_team_name,
        dd_ncb_trxl_currency_cd,
        dd_ncb_conversion_type_cd,
        dd_ncb_so_src_crt_dtm,
        dd_ncb_sk_sales_ord_hdr_id_int,
        dd_ncb_ss_cd,
        dd_ncb_cancelled_flg,
        dd_ncb_sales_order_cat_type,
        dd_ncb_bk_sales_order_num_int,
        dd_ncb_bk_order_type_name,
        dd_ncb_order_dtm,
        dd_ncb_shipment_priority_cd,
        dd_ncb_sold_to_cust_acct_key,
        dd_ncb_purchase_order_num,
        fiscal_year_month_int,
        edw_update_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_user,
        dd_ncb_so_crtd_by_erp_usr_name,
        dd_so_program_type_name
    FROM {{ source('raw', 'n_so_ncb_tv') }}
),

final AS (
    SELECT
        sales_order_key,
        start_tv_dt,
        end_tv_dt,
        dv_ncb_interntnl_demo_flg,
        dv_ncb_replacement_demo_flg,
        dv_ncb_revenue_flg,
        dv_ncb_acquisition_flg,
        dv_ncb_ic_revenue_flg,
        dd_ncb_bill_to_customer_key,
        dd_ncb_oracle_book_dtm,
        dd_ncb_cst_srvc_rep_team_name,
        dd_ncb_trxl_currency_cd,
        dd_ncb_conversion_type_cd,
        dd_ncb_so_src_crt_dtm,
        dd_ncb_sk_sales_ord_hdr_id_int,
        dd_ncb_ss_cd,
        dd_ncb_cancelled_flg,
        dd_ncb_sales_order_cat_type,
        dd_ncb_bk_sales_order_num_int,
        dd_ncb_bk_order_type_name,
        dd_ncb_order_dtm,
        dd_ncb_shipment_priority_cd,
        dd_ncb_sold_to_cust_acct_key,
        dd_ncb_purchase_order_num,
        fiscal_year_month_int,
        edw_update_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_user,
        dd_ncb_so_crtd_by_erp_usr_name,
        dd_so_program_type_name
    FROM source_n_so_ncb_tv
)

SELECT * FROM final