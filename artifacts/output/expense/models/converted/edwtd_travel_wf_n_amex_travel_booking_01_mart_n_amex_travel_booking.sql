{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_amex_travel_booking', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_N_AMEX_TRAVEL_BOOKING',
        'target_table': 'N_AMEX_TRAVEL_BOOKING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.143376+00:00'
    }
) }}

WITH 

source_w_amex_travel_booking AS (
    SELECT
        bk_amex_pnr_locator_id,
        bk_amex_gds_cd,
        bk_amex_customer_id,
        bk_amex_pax_sequence_int,
        trip_start_dt,
        amex_travel_booking_dt,
        traveler_cisco_wrkr_party_key,
        bk_reason_for_travel_cd,
        financial_department_cd,
        financial_dept_company_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        booking_agent_id_num,
        ticketing_agent_id_num,
        initial_car_rental_rprtng_amt,
        reporting_currency_cd,
        initial_car_days_reserved_cnt,
        initial_hotel_reporting_amt,
        initial_hotel_nights_cnt,
        itinerary_descr,
        travel_agency_phone_num,
        rprtd_passngr_business_ph_num,
        rprtd_passngr_home_ph_num,
        rprtd_passngr_fax_num,
        passenger_return_dt,
        seat_type_preference_cd,
        statement_num,
        ticketed_flg,
        source_deleted_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_amex_travel_booking') }}
),

final AS (
    SELECT
        bk_amex_pnr_locator_id,
        bk_amex_gds_cd,
        bk_amex_customer_id,
        bk_amex_pax_sequence_int,
        trip_start_dt,
        amex_travel_booking_dt,
        traveler_cisco_wrkr_party_key,
        bk_reason_for_travel_cd,
        financial_department_cd,
        financial_dept_company_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        booking_agent_id_num,
        ticketing_agent_id_num,
        initial_car_rental_rprtng_amt,
        reporting_currency_cd,
        initial_car_days_reserved_cnt,
        initial_hotel_reporting_amt,
        initial_hotel_nights_cnt,
        itinerary_descr,
        travel_agency_phone_num,
        rprtd_passngr_business_ph_num,
        rprtd_passngr_home_ph_num,
        rprtd_passngr_fax_num,
        passenger_return_dt,
        seat_type_preference_cd,
        statement_num,
        ticketed_flg,
        source_deleted_flg
    FROM source_w_amex_travel_booking
)

SELECT * FROM final