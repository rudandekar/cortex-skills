{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_amex_travel_bkg_detail_hotel', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_WK_AMEX_TRAVEL_BKG_DETAIL_HOTEL',
        'target_table': 'W_AMEX_TRAVEL_BKG_DETAIL_HOTEL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.715864+00:00'
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
        bk_amex_pnr_locator_id,
        bk_amex_gds_cd,
        bk_amex_customer_id,
        bk_amex_pax_sequence_int,
        bk_hotel_sequence_num_int,
        hotel_room_local_rt_amt,
        hotel_room_reporting_rt_amt,
        hotel_name,
        hotel_arrival_dt,
        hotel_checkout_dt,
        hotel_room_night_cnt,
        hotel_room_cnt,
        hotel_state_cd,
        hotel_city_name,
        local_currency_cd,
        hotel_country_cd,
        reporting_currency_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        hotel_room_booking_status_cd,
        hotel_chain_cd,
        hotel_room_booking_conf_num,
        hotel_domestic_intrntnl_cd,
        hotel_booking_guest_cnt,
        hotel_room_type_cd,
        hotel_rate_type_cd,
        hotel_property_num,
        hotel_street_addr,
        hotel_postal_cd,
        hotel_city_cd,
        hotel_phone_num,
        hotel_compliance_reason_cd,
        source_deleted_flg,
        action_code,
        dml_type
    FROM source_st_booked_hotel
)

SELECT * FROM final