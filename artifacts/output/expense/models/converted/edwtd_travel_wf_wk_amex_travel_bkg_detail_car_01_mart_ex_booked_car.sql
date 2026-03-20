{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_amex_travel_bkg_detail_car', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_WK_AMEX_TRAVEL_BKG_DETAIL_CAR',
        'target_table': 'EX_BOOKED_CAR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.042472+00:00'
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
        action_code,
        exception_type
    FROM source_st_booked_car
)

SELECT * FROM final