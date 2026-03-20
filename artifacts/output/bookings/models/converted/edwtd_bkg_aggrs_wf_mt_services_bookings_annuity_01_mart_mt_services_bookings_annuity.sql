{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_services_bookings_annuity', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_MT_SERVICES_BOOKINGS_ANNUITY',
        'target_table': 'MT_SERVICES_BOOKINGS_ANNUITY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.679645+00:00'
    }
) }}

WITH 

source_mt_services_bookings_annuity AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        sales_territory_key,
        sales_rep_num,
        goods_product_key,
        dv_fiscal_yr_mnth_week_num_int,
        true_up_flg,
        fiscal_week_name,
        fiscal_year_month_int,
        annuity_amt,
        dv_my_net_amt,
        dv_ca_service_boookings_amt,
        dv_annual_net_amt,
        dv_annualized_flg,
        booking_type_cd,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        dv_fiscal_year_mth_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_tss_alctn_mthd_type_id_int,
        summary_quote_flg,
        dv_cx_product
    FROM {{ source('raw', 'mt_services_bookings_annuity') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_dt,
        bkgs_measure_trans_type_cd,
        sales_territory_key,
        sales_rep_num,
        goods_product_key,
        dv_fiscal_yr_mnth_week_num_int,
        true_up_flg,
        fiscal_week_name,
        fiscal_year_month_int,
        annuity_amt,
        dv_my_net_amt,
        dv_ca_service_boookings_amt,
        dv_annual_net_amt,
        dv_annualized_flg,
        booking_type_cd,
        ru_service_contract_start_dtm,
        ru_service_contract_end_dtm,
        dv_fiscal_year_mth_number_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_tss_alctn_mthd_type_id_int,
        summary_quote_flg,
        dv_cx_product
    FROM source_mt_services_bookings_annuity
)

SELECT * FROM final