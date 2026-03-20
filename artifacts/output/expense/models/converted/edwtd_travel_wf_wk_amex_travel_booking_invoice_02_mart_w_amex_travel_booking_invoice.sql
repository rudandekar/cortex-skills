{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_amex_travel_booking_invoice', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_WK_AMEX_TRAVEL_BOOKING_INVOICE',
        'target_table': 'W_AMEX_TRAVEL_BOOKING_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.906022+00:00'
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
        bk_amex_pnr_locator_id,
        bk_amex_customer_id,
        bk_amex_pax_sequence_int,
        bk_amex_gds_cd,
        gross_fare_reporting_amt,
        total_tkt_grss_fare_rprtg_amt,
        gross_fare_local_amt,
        lowest_fare_reporting_amt,
        form_of_payment_cd,
        full_fare_reporting_amt,
        first_ticket_num,
        ticket_prepaid_flg,
        validating_carrier_cd,
        reason_cd,
        domestic_international_cd,
        amex_travel_booking_invoice_dt,
        local_currency_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        source_deleted_flg,
        action_code,
        dml_type
    FROM source_st_booked_invoice
)

SELECT * FROM final