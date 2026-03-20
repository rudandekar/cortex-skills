{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_tss_service_bkgs_measure_qtr', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_MT_TSS_SERVICE_BKGS_MEASURE_QTR',
        'target_table': 'MT_TSS_SERVICE_BKGS_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.996058+00:00'
    }
) }}

WITH 

source_wi_qtr_tss_bkgs_measure AS (
    SELECT
        bookings_measure_key,
        dv_fiscal_year_mth_number_int,
        bkgs_measure_trans_type_code,
        tss_allctn_method_type_id_int,
        bookings_process_date,
        service_product_key,
        goods_product_key,
        bk_so_number_int,
        trade_in_amount,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        dd_comp_us_hold_net_price_amt,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amount,
        dd_extended_hold_quantity,
        dd_comp_us_standard_price_amt
    FROM {{ source('raw', 'wi_qtr_tss_bkgs_measure') }}
),

final AS (
    SELECT
        bookings_measure_key,
        dv_fiscal_year_mth_number_int,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        goods_product_key,
        tss_allctn_method_type_id_int,
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
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        dv_corporate_booking_flg
    FROM source_wi_qtr_tss_bkgs_measure
)

SELECT * FROM final