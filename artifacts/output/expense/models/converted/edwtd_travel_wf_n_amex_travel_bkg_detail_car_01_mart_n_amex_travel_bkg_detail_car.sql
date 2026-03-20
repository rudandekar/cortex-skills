{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_amex_travel_bkg_detail_car', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_N_AMEX_TRAVEL_BKG_DETAIL_CAR',
        'target_table': 'N_AMEX_TRAVEL_BKG_DETAIL_CAR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.866012+00:00'
    }
) }}

WITH 

source_w_amex_travel_bkg_detail_car AS (
    SELECT
        bk_amex_pnr_locator_id,
        bk_amex_gds_cd,
        bk_amex_customer_id,
        bk_amex_pax_sequence_int,
        bk_rental_car_seq_num_int,
        bk_rental_company_cd,
        rental_car_pickup_dt,
        rental_car_local_rt_amt,
        rental_car_reporting_rt_amt,
        rental_car_cnt,
        rental_car_drop_off_dt,
        rental_car_state_cd,
        rental_car_country_cd,
        bk_rental_car_type_cd,
        local_currency_cd,
        reporting_currency_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        rental_car_booking_dt,
        rental_car_booking_status_cd,
        rental_car_city_name,
        rental_car_city_cd,
        rental_car_confirmation_num,
        rental_car_domestc_intrntnl_cd,
        rental_car_pickup_location_cd,
        source_deleted_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_amex_travel_bkg_detail_car') }}
),

final AS (
    SELECT
        bk_amex_pnr_locator_id,
        bk_amex_gds_cd,
        bk_amex_customer_id,
        bk_amex_pax_sequence_int,
        bk_rental_car_seq_num_int,
        bk_rental_company_cd,
        rental_car_pickup_dt,
        rental_car_local_rt_amt,
        rental_car_reporting_rt_amt,
        rental_car_cnt,
        rental_car_drop_off_dt,
        rental_car_state_cd,
        rental_car_country_cd,
        bk_rental_car_type_cd,
        local_currency_cd,
        reporting_currency_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        rental_car_booking_dt,
        rental_car_booking_status_cd,
        rental_car_city_name,
        rental_car_city_cd,
        rental_car_confirmation_num,
        rental_car_domestc_intrntnl_cd,
        rental_car_pickup_location_cd,
        source_deleted_flg
    FROM source_w_amex_travel_bkg_detail_car
)

SELECT * FROM final