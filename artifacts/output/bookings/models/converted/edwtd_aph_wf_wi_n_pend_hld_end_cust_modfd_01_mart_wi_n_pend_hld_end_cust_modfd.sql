{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_n_pend_hld_end_cust_modfd', 'batch', 'edwtd_aph'],
    meta={
        'source_workflow': 'wf_m_WI_N_PEND_HLD_END_CUST_MODFD',
        'target_table': 'WI_N_PEND_HLD_END_CUST_MODFD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.669334+00:00'
    }
) }}

WITH 

source_wi_sol_end_cust_modified_aph AS (
    SELECT
        sales_order_line_key,
        end_customer_key,
        start_tv_datetime,
        start_ssp_date,
        end_tv_datetime,
        end_ssp_date,
        end_customer_type_code,
        ss_code,
        end_customer_assgn_level,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'wi_sol_end_cust_modified_aph') }}
),

source_n_drvd_accept_pending_hold AS (
    SELECT
        drvd_ncr_bkg_trx_key,
        transaction_dtm,
        transaction_sequence_id_int,
        ss_cd,
        process_dt,
        bookings_pct,
        split_pct,
        trade_in_amt,
        dd_ah_bk_sales_rep_num,
        dd_ah_bk_sales_credit_type_cd,
        dd_ah_rma_flg,
        dv_ah_bkg_pct,
        dv_ah_international_demo_flg,
        dv_ah_replacement_demo_flg,
        dv_ah_revenue_flg,
        dd_ah_overlay_flg,
        dv_ah_ic_revenue_flg,
        dv_ah_charges_flg,
        dv_ah_misc_flg,
        dv_ah_acquisition_flg,
        dv_ah_service_flg,
        dd_ah_corp_bkg_flag,
        dd_ah_cancelled_flg,
        dv_ah_comp_net_price_usd_amt,
        dv_ah_comp_list_price_usd_amt,
        dv_ah_comp_cost_usd_amt,
        dv_ah_extended_qty,
        dd_ah_sales_territory_key,
        dd_ah_trxl_currency_cd,
        dd_ah_comp_us_std_price_amt,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        bk_fiscal_calendar_cd,
        dv_fiscal_year_mth_number_int,
        sales_order_key,
        dd_ah_ship_to_customer_key,
        dd_ah_bill_to_customer_key,
        dd_ah_sold_to_customer_key,
        dd_ah_end_customer_key,
        partner_site_party_key,
        dd_ah_product_key,
        sales_order_line_key,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM {{ source('raw', 'n_drvd_accept_pending_hold') }}
),

final AS (
    SELECT
        drvd_ncr_bkg_trx_key,
        end_customer_key,
        process_date,
        edw_create_datetime
    FROM source_n_drvd_accept_pending_hold
)

SELECT * FROM final