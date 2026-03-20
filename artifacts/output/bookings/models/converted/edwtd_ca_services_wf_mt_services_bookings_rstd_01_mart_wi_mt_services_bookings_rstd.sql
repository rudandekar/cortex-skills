{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_mt_services_bookings_rstd', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_WI_MT_SERVICES_BOOKINGS_RSTD',
        'target_table': 'WI_MT_SERVICES_BOOKINGS_RSTD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.930092+00:00'
    }
) }}

WITH 

source_wi_mt_services_bookings_rstd AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        goods_product_key,
        dv_fiscal_year_mth_number_int,
        dv_tss_alctn_mthd_type_id_int,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        corporate_bookings_flg,
        trade_in_amt,
        dd_comp_us_net_price_amt,
        dd_comp_us_list_price_amt,
        dd_comp_us_cost_amt,
        dd_extended_qty,
        dd_comp_us_hold_net_price_amt,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amt,
        dd_extended_hold_qty,
        dd_comp_us_standard_price_amt,
        sales_territory_key,
        sales_rep_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        service_item_key
    FROM {{ source('raw', 'wi_mt_services_bookings_rstd') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        goods_product_key,
        dv_fiscal_year_mth_number_int,
        dv_tss_alctn_mthd_type_id_int,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        corporate_bookings_flg,
        trade_in_amt,
        dd_comp_us_net_price_amt,
        dd_comp_us_list_price_amt,
        dd_comp_us_cost_amt,
        dd_extended_qty,
        dd_comp_us_hold_net_price_amt,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amt,
        dd_extended_hold_qty,
        dd_comp_us_standard_price_amt,
        sales_territory_key,
        sales_rep_number,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        service_item_key
    FROM source_wi_mt_services_bookings_rstd
)

SELECT * FROM final