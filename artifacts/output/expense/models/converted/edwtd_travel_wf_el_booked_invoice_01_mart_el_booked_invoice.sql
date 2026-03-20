{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_booked_invoice', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_EL_BOOKED_INVOICE',
        'target_table': 'EL_BOOKED_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.071261+00:00'
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
        deleted_flag,
        create_datetime,
        update_datetime,
        ticket_gross_fare
    FROM source_st_booked_invoice
)

SELECT * FROM final