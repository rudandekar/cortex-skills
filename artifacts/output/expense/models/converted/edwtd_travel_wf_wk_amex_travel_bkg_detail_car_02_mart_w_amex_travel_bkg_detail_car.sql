{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_amex_travel_bkg_detail_car', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_WK_AMEX_TRAVEL_BKG_DETAIL_CAR',
        'target_table': 'W_AMEX_TRAVEL_BKG_DETAIL_CAR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.085650+00:00'
    }
) }}

WITH 

source_st_booked_car AS (
    SELECT
        batch_id,
        xpk_row3,
        fk_travel3,
        confirmation0,
        car_type,
        pax_sequence3,
        booking_status0,
        converted_currency1,
        original_amount0,
        country0,
        dom_intl0,
        pnr_locator3,
        customer_id3,
        converted_amount1,
        chain0,
        booking_date1,
        city0,
        original_currency1,
        number_cars,
        pickup_date,
        gds_code3,
        state0,
        car_sequence,
        dropoff_date,
        reason_code0,
        booking_type0,
        pickup_location,
        city_code0,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_booked_car') }}
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
        source_deleted_flg,
        action_code,
        dml_type
    FROM source_st_booked_car
)

SELECT * FROM final