{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_fdi_rstd_bookings_fqtr', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_MT_FDI_RSTD_BOOKINGS_FQTR',
        'target_table': 'MT_FDI_RSTD_BOOKINGS_FQTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.436092+00:00'
    }
) }}

WITH 

source_mt_fdi_rstd_bookings_fqtr AS (
    SELECT
        fiscal_year_quarter_num_int,
        product_key,
        sales_territory_key,
        sales_rep_num,
        cancelled_flg,
        acquisition_flg,
        corporate_bookings_flg,
        service_flg,
        revenue_flg,
        dd_comp_us_net_price_amt,
        dd_comp_us_list_price_amt,
        dd_comp_us_cost_amt,
        dd_extended_qty,
        dd_comp_us_hold_net_price_amt,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amt,
        dd_extended_hold_qty,
        dd_comp_us_standard_price_amt,
        acquisition_source_cd,
        dv_bookings_type,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        dv_attribution_cd,
        dv_product_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_fmv_flg
    FROM {{ source('raw', 'mt_fdi_rstd_bookings_fqtr') }}
),

final AS (
    SELECT
        fiscal_year_quarter_num_int,
        product_key,
        sales_territory_key,
        sales_rep_num,
        cancelled_flg,
        acquisition_flg,
        corporate_bookings_flg,
        service_flg,
        revenue_flg,
        dd_comp_us_net_price_amt,
        dd_comp_us_list_price_amt,
        dd_comp_us_cost_amt,
        dd_extended_qty,
        dd_comp_us_hold_net_price_amt,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amt,
        dd_extended_hold_qty,
        dd_comp_us_standard_price_amt,
        acquisition_source_cd,
        dv_bookings_type,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        dv_attribution_cd,
        dv_product_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_fmv_flg
    FROM source_mt_fdi_rstd_bookings_fqtr
)

SELECT * FROM final