{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_amex_travel_booking_invoice', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_WK_AMEX_TRAVEL_BOOKING_INVOICE',
        'target_table': 'EX_BOOKED_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.736250+00:00'
    }
) }}

WITH 

source_st_booked_invoice AS (
    SELECT
        batch_id,
        xpk_row4,
        fk_travel4,
        card_number,
        card_company,
        customer_id4,
        ticket_prepaid,
        ticket_number,
        gross_air_converted_amount,
        fare_lowest,
        account_number,
        validating_airline,
        full_fare,
        fare_gross,
        card_expiration,
        dom_intl_flag,
        invoice_date,
        converted_currency2,
        reason_code1,
        pnr_locator4,
        original_currency2,
        gds_code4,
        form_of_payment,
        pax_sequence4,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_booked_invoice') }}
),

final AS (
    SELECT
        batch_id,
        xpk_row4,
        fk_travel4,
        card_number,
        card_company,
        customer_id4,
        ticket_prepaid,
        ticket_number,
        gross_air_converted_amount,
        fare_lowest,
        account_number,
        validating_airline,
        full_fare,
        fare_gross,
        card_expiration,
        dom_intl_flag,
        invoice_date,
        converted_currency2,
        reason_code1,
        pnr_locator4,
        original_currency2,
        gds_code4,
        form_of_payment,
        pax_sequence4,
        create_datetime,
        action_code,
        exception_type
    FROM source_st_booked_invoice
)

SELECT * FROM final