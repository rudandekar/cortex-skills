{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_amex_travel_bkg_detail_hotel', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_WK_AMEX_TRAVEL_BKG_DETAIL_HOTEL',
        'target_table': 'EX_BOOKED_HOTEL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.682254+00:00'
    }
) }}

WITH 

source_st_booked_hotel AS (
    SELECT
        batch_id,
        xpk_row2,
        fk_travel2,
        state,
        confirmation,
        converted_currency0,
        gds_code2,
        address,
        city,
        city_code,
        country,
        converted_amount0,
        number_guests,
        customer_id2,
        pax_sequence2,
        chain,
        number_nights,
        rate_type,
        zipcode,
        booking_date0,
        number_rooms,
        original_currency0,
        room_type,
        property,
        checkout_date,
        pnr_locator2,
        dom_intl,
        hotel_name,
        booking_type,
        original_amount,
        reason_code,
        booking_status,
        phone,
        checkin_date,
        htl_sequence,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_booked_hotel') }}
),

final AS (
    SELECT
        batch_id,
        xpk_row2,
        fk_travel2,
        state,
        confirmation,
        converted_currency0,
        gds_code2,
        address,
        city,
        city_code,
        country,
        converted_amount0,
        number_guests,
        customer_id2,
        pax_sequence2,
        chain,
        number_nights,
        rate_type,
        zipcode,
        booking_date0,
        number_rooms,
        original_currency0,
        room_type,
        property,
        checkout_date,
        pnr_locator2,
        dom_intl,
        hotel_name,
        booking_type,
        original_amount,
        reason_code,
        booking_status,
        phone,
        checkin_date,
        htl_sequence,
        create_datetime,
        action_code,
        exception_type
    FROM source_st_booked_hotel
)

SELECT * FROM final