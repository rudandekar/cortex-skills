{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_sol_ncb_tv_xaas', 'batch', 'edwtd_xaas_bkgs'],
    meta={
        'source_workflow': 'wf_m_N_SOL_NCB_TV_XAAS',
        'target_table': 'N_SOL_NCB_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.410803+00:00'
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
        order_dtm,
        so_sbscrptn_itm_sls_trx_key,
        trx_type_cd,
        erp_annualized_bkg_trx_key,
        xaas_annualized_bkg_trx_key,
        transactional_acv_flg,
        history_flg,
        dv_orig_ncb_comp_net_usd_amt
    FROM {{ source('raw', 'n_sol_ncb_tv') }}
),

final AS (
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
        order_dtm,
        so_sbscrptn_itm_sls_trx_key,
        trx_type_cd,
        erp_annualized_bkg_trx_key,
        xaas_annualized_bkg_trx_key,
        transactional_acv_flg,
        history_flg,
        dv_orig_ncb_comp_net_usd_amt
    FROM source_n_sol_ncb_tv
)

SELECT * FROM final