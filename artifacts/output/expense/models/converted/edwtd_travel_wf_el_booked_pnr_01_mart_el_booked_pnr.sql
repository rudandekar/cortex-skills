{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_booked_pnr', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_EL_BOOKED_PNR',
        'target_table': 'EL_BOOKED_PNR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.877112+00:00'
    }
) }}

WITH 

source_st_booked_pnr AS (
    SELECT
        batch_id,
        xpk_row,
        fk_travel,
        hotel_nights,
        car_converted_amount,
        depart_date,
        itinerary,
        queue_recvd_in,
        phone_default,
        car_days,
        seat_preference,
        statement1,
        agent_ticketing,
        booking_date,
        cancel_indicator,
        hotel_converted_amount,
        pcc_receiving,
        pcc_booking,
        remark,
        return_date,
        customer_id,
        phone_agency,
        gds_code,
        pax_sequence,
        ticketed_indicator,
        pulled_date,
        pnr_locator,
        phone_fax,
        phone_home,
        agent_id,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_booked_pnr') }}
),

final AS (
    SELECT
        xpk_row,
        fk_travel,
        hotel_nights,
        car_converted_amount,
        depart_date,
        itinerary,
        queue_recvd_in,
        phone_default,
        car_days,
        seat_preference,
        statement1,
        agent_ticketing,
        booking_date,
        cancel_indicator,
        hotel_converted_amount,
        pcc_receiving,
        pcc_booking,
        remark,
        return_date,
        customer_id,
        phone_agency,
        gds_code,
        pax_sequence,
        ticketed_indicator,
        pulled_date,
        pnr_locator,
        phone_fax,
        phone_home,
        agent_id,
        deleted_flag,
        create_datetime,
        update_datetime,
        department_code,
        emplid,
        udid_info_num8
    FROM source_st_booked_pnr
)

SELECT * FROM final