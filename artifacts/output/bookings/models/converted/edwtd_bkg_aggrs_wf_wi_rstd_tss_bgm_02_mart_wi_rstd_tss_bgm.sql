{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_tss_bgm_interm', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_TSS_BGM_INTERM',
        'target_table': 'WI_RSTD_TSS_BGM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.249014+00:00'
    }
) }}

WITH 

source_st_rstd_tss_bgm_interm AS (
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
    FROM {{ source('raw', 'st_rstd_tss_bgm_interm') }}
),

source_st_rstd_tss_bgm AS (
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
    FROM {{ source('raw', 'st_rstd_tss_bgm') }}
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
    FROM source_st_rstd_tss_bgm
)

SELECT * FROM final