{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_order_finance_bi', 'batch', 'edwtd_finance_reporting'],
    meta={
        'source_workflow': 'wf_m_W_SALES_ORDER_FINANCE_BI',
        'target_table': 'W_SALES_ORDER_FINANCE_BI',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.947778+00:00'
    }
) }}

WITH 

source_wi_bkg_measure_so_bif AS (
    SELECT
        sales_order_key,
        dd_comp_us_net_price_amount,
        so_discount_pct,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'wi_bkg_measure_so_bif') }}
),

source_n_bookings_measure AS (
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
        edw_update_datetime
    FROM {{ source('raw', 'n_bookings_measure') }}
),

final AS (
    SELECT
        sales_order_key,
        dv_discount_band_cd,
        dv_order_band_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_n_bookings_measure
)

SELECT * FROM final