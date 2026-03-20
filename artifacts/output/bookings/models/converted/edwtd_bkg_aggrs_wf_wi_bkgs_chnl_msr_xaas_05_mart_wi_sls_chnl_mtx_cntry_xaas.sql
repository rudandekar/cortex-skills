{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_bkgs_chnl_msr_xaas', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_BKGS_CHNL_MSR_XAAS',
        'target_table': 'WI_SLS_CHNL_MTX_CNTRY_XAAS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.066959+00:00'
    }
) }}

WITH 

source_wi_sls_chnl_mtx_drop_ship_xaas AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        end_customer_type_code,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        dv_end_cust_party_key,
        partner_site_party_key_int,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        prtn_cntry_cd,
        dv_end_cust_cntry_cd,
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        end_cust_party_key,
        sales_channel_code,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sls_chnl_mtx_drop_ship_xaas') }}
),

source_wi_chnl_bkgs_msr_for_xaas_incr AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        end_customer_type_code,
        bill_to_customer_key,
        process_date,
        edw_create_user,
        edw_create_datetime,
        sales_channel_code,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_chnl_bkgs_msr_for_xaas_incr') }}
),

source_wi_sls_chnl_mtx_cntry_xaas AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        end_customer_type_code,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        dv_end_cust_party_key,
        partner_site_party_key_int,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        prtn_cntry_cd,
        dv_end_cust_cntry_cd,
        sales_channel_code,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_sls_chnl_mtx_cntry_xaas') }}
),

source_wi_sales_channel_matrix_xaas AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        bill_to_customer_key,
        partner_site_party_key,
        channel_drop_ship_flg,
        sales_channel_booking_flg,
        edw_create_user,
        edw_create_datetime
    FROM {{ source('raw', 'wi_sales_channel_matrix_xaas') }}
),

source_n_bkgs_measure AS (
    SELECT
        bookings_measure_key,
        sales_order_key,
        sales_order_line_key,
        product_key,
        ar_trx_line_key,
        ar_trx_key,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_end_customer_key,
        transaction_datetime,
        sales_territory_key,
        sales_rep_number,
        bookings_process_date,
        dv_fiscal_year_mth_number_int,
        bk_pos_transaction_id_int,
        bk_sales_adj_line_number_int,
        bk_sales_adj_number_int,
        adjustment_type_code,
        sales_channel_code,
        sales_credit_type_code,
        ide_adjustment_code,
        adjustment_code,
        bkgs_measure_trans_type_code,
        cancelled_flg,
        cancel_code,
        acquisition_flg,
        forward_reverse_flg,
        distributor_offset_flg,
        corporate_bookings_flg,
        overlay_flg,
        ic_revenue_flg,
        charges_flg,
        salesrep_flg,
        misc_flg,
        service_flg,
        international_demo_flg,
        replacement_demo_flg,
        revenue_flg,
        rma_flg,
        trade_in_amount,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        dd_comp_us_hold_net_price_amt,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amount,
        dd_extended_hold_quantity,
        dd_comp_us_standard_price_amt,
        wips_originator_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        conversion_rt,
        conversion_dt,
        dd_bk_so_number_int,
        dd_cisco_booked_dtm,
        dd_sales_order_category_type,
        dd_sls_ord_operating_unit_cd,
        dd_trx_currency_cd,
        dv_transaction_type,
        adjustment_descr_key,
        dv_transaction_name,
        bookings_split_pct,
        dv_source_order_num_int,
        dv_deal_id,
        dv_purchase_order_num,
        dv_booked_dt
    FROM {{ source('raw', 'n_bkgs_measure') }}
),

final AS (
    SELECT
        sales_territory_key,
        sold_to_customer_key,
        end_customer_key,
        dv_end_customer_key,
        end_customer_type_code,
        bill_to_customer_key,
        sold_to_party_key,
        bill_to_party_key,
        dv_end_cust_party_key,
        partner_site_party_key_int,
        partner,
        sold_to_grand_prnt_pty_key,
        bill_to_grand_prnt_pty_key,
        edw_create_user,
        edw_create_datetime,
        prtn_cntry_cd,
        dv_end_cust_cntry_cd,
        sales_channel_code,
        ship_to_customer_key
    FROM source_n_bkgs_measure
)

SELECT * FROM final