{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_mt_bkgs_by_month', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_W_MT_BKGS_BY_MONTH',
        'target_table': 'W_MT_BKGS_BY_MONTH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.305365+00:00'
    }
) }}

WITH 

source_w_mt_bkgs_by_month AS (
    SELECT
        fiscal_year_month_number_int,
        product_key,
        sales_territory_key,
        sales_rep_num,
        bill_to_customer_key,
        sold_to_customer_key,
        ship_to_customer_key,
        end_customer_key,
        bkgs_measure_trans_type_cd,
        partner_party_site_key,
        wips_originator_id_int,
        salesrep_flg,
        service_flg,
        channel_booking_flg,
        extended_hold_qty,
        extended_qty,
        comp_us_list_price_amt,
        comp_us_hold_list_price_amt,
        comp_us_hold_net_price_amt,
        comp_us_hold_cost_amt,
        comp_us_net_price_amt,
        comp_us_cost_amt,
        comp_us_standard_price_amt,
        trade_in_amt,
        channel_drop_ship_flg,
        cancel_cd,
        dv_drct_val_added_dsti_ord_flg,
        corporate_bookings_flg,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        dv_attribution_cd,
        dv_product_key,
        dv_fmv_flg,
        dsv_flg,
        dv_ato_product_key,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_mt_bkgs_by_month') }}
),

final AS (
    SELECT
        fiscal_year_month_number_int,
        product_key,
        sales_territory_key,
        sales_rep_num,
        bill_to_customer_key,
        sold_to_customer_key,
        ship_to_customer_key,
        end_customer_key,
        bkgs_measure_trans_type_cd,
        partner_party_site_key,
        wips_originator_id_int,
        salesrep_flg,
        service_flg,
        channel_booking_flg,
        extended_hold_qty,
        extended_qty,
        comp_us_list_price_amt,
        comp_us_hold_list_price_amt,
        comp_us_hold_net_price_amt,
        comp_us_hold_cost_amt,
        comp_us_net_price_amt,
        comp_us_cost_amt,
        comp_us_standard_price_amt,
        trade_in_amt,
        channel_drop_ship_flg,
        cancel_cd,
        dv_drct_val_added_dsti_ord_flg,
        corporate_bookings_flg,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        dv_attribution_cd,
        dv_product_key,
        dv_fmv_flg,
        dsv_flg,
        dv_ato_product_key,
        action_code,
        dml_type
    FROM source_w_mt_bkgs_by_month
)

SELECT * FROM final