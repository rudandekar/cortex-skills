{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_scao_ncb_tv', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_N_SCAO_NCB_TV',
        'target_table': 'N_SCAO_NCB_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.796537+00:00'
    }
) }}

WITH 

source_n_sol_ncb_tv AS (
    SELECT
        sales_order_line_key,
        start_tv_dt,
        end_tv_dt,
        dv_ncb_extended_qty,
        dv_ncb_comp_net_prc_usd_amt,
        dv_ncb_comp_list_prc_usd_amt,
        dv_ncb_comp_cost_usd_amt,
        dv_ncb_charges_flg,
        dv_ncb_misc_flg,
        dv_ncb_service_flg,
        dv_ncb_cfs_credit_chk_pend_flg,
        dv_ncb_ic_special_discount_flg,
        dd_ncb_product_key,
        dd_ncb_invoiced_qty_amt,
        dd_ncb_sk_sls_ord_line_id_int,
        dd_ncb_ss_cd,
        dd_ncb_bk_sales_order_num_int,
        dd_ncb_order_qty,
        dd_ncb_promised_cust_ship_dt,
        dd_ncb_ship_to_customer_key,
        dd_ncb_so_line_source_crt_dtm,
        dv_ncb_order_status_cd,
        bk_ship_from_inv_org_name_code,
        fiscal_year_month_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        conversion_rt,
        order_dtm
    FROM {{ source('raw', 'n_sol_ncb_tv') }}
),

final AS (
    SELECT
        conversion_dt,
        conversion_rt,
        dd_nc_bill_to_customer_key,
        dd_nc_bk_sales_credit_type_cd,
        dd_nc_bk_sales_rep_num,
        dd_nc_cancelled_flg,
        dd_nc_dv_overlay_flg,
        dd_nc_end_customer_key,
        dd_nc_partner_site_party_key,
        dd_nc_product_key,
        dd_nc_sales_commission_pct,
        dd_nc_sales_order_key,
        dd_nc_sales_order_line_key,
        dd_nc_sales_territory_key,
        dd_nc_ship_to_customer_key,
        dd_nc_sk_trx_sc_id_int,
        dd_nc_sold_to_cust_account_key,
        dd_nc_ss_cd,
        dv_nc_acquisition_flg,
        dv_nc_cfs_credit_chk_pend_flg,
        dv_nc_channel_ncb_flg,
        dv_nc_charges_flg,
        dv_nc_comp_cost_usd_amt,
        dv_nc_comp_net_price_usd_amt,
        dv_nc_comp_std_price_usd_amt,
        dv_nc_computed_lst_prc_usd_amt,
        dv_nc_corporate_ncb_flg,
        dv_nc_extended_qty,
        dv_nc_fiscal_yr_month_num_int,
        dv_nc_ic_revenue_flg,
        dv_nc_ic_special_discount_flg,
        dv_nc_international_demo_flg,
        dv_nc_misc_flg,
        dv_nc_process_dt,
        dv_nc_replacement_demo_flg,
        dv_nc_revenue_flg,
        dv_nc_rma_flg,
        dv_nc_service_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        end_tv_dt,
        sales_credit_asgn_apld_key,
        start_tv_dt
    FROM source_n_sol_ncb_tv
)

SELECT * FROM final