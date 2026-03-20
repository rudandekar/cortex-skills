{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_bookings_by_sol_by_mth_i', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_BOOKINGS_BY_SOL_BY_MTH_I',
        'target_table': 'WI_BOOKINGS_BY_SOL_BY_MTH_M',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.447811+00:00'
    }
) }}

WITH 

source_wi_bookings_by_sol_by_mth_m AS (
    SELECT
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        sales_order_key,
        sales_order_line_key,
        bookings_product_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_end_customer_key,
        end_customer_key,
        sales_territory_key,
        bkgs_measure_trans_type_code,
        corporate_bookings_flg,
        service_flg,
        wips_originator_id_int,
        dv_fiscal_year_mth_number_int,
        partner_site_party_key,
        dv_drct_val_added_dsti_ord_flg,
        channel_bookings_flg,
        channel_drop_ship_flg
    FROM {{ source('raw', 'wi_bookings_by_sol_by_mth_m') }}
),

final AS (
    SELECT
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        sales_order_key,
        sales_order_line_key,
        bookings_product_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_end_customer_key,
        end_customer_key,
        sales_territory_key,
        bkgs_measure_trans_type_code,
        corporate_bookings_flg,
        service_flg,
        wips_originator_id_int,
        dv_fiscal_year_mth_number_int,
        partner_site_party_key,
        dv_drct_val_added_dsti_ord_flg,
        channel_bookings_flg,
        channel_drop_ship_flg,
        dv_revenue_recognition_flg,
        dv_net_spread_flg
    FROM source_wi_bookings_by_sol_by_mth_m
)

SELECT * FROM final