{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_rstd_tss_bgm', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_W_RSTD_TSS_BGM',
        'target_table': 'W_RSTD_TSS_BGM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.345065+00:00'
    }
) }}

WITH 

source_wi_rstd_tss_bgm AS (
    SELECT
        fiscal_year_number_int,
        fiscal_year_quarter_number_int,
        dv_fiscal_year_mth_number_int,
        fiscal_year_week_num_int,
        bookings_measure_key,
        bkgs_measure_trans_type_code,
        bookings_process_date,
        sales_territory_key,
        sales_rep_number,
        dv_source_order_num_int,
        product_key,
        dv_product_key,
        corporate_bookings_flg,
        dv_revenue_recognition_flg,
        dd_comp_us_net_price_amount,
        dd_comp_us_standard_price_amt,
        dd_comp_us_list_price_amount,
        dv_bkg_gross_mgn_amount,
        dv_bkg_rebate_amt,
        dv_bgm_fx_cost_amount,
        dv_fx_net_price_amt,
        dv_fx_rebate_price_amt,
        tss_country_factor_key,
        end_customer_key,
        product_type,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM {{ source('raw', 'wi_rstd_tss_bgm') }}
),

final AS (
    SELECT
        fiscal_year_number_int,
        fiscal_year_quarter_number_int,
        dv_fiscal_year_mth_number_int,
        fiscal_year_week_num_int,
        bookings_measure_key,
        bkgs_measure_trans_type_code,
        bookings_process_date,
        sales_territory_key,
        sales_rep_number,
        dv_source_order_num_int,
        product_key,
        dv_product_key,
        corporate_bookings_flg,
        dv_revenue_recognition_flg,
        dv_bkg_gross_mgn_amount,
        dv_bkg_rebate_amt,
        dv_bgm_fx_cost_amount,
        dv_fx_net_price_amt,
        dv_fx_rebate_price_amt,
        tss_country_factor_key,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        action_code,
        dml_type
    FROM source_wi_rstd_tss_bgm
)

SELECT * FROM final