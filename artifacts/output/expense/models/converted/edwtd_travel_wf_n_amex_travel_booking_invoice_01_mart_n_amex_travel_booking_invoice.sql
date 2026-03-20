{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_amex_travel_booking_invoice', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_N_AMEX_TRAVEL_BOOKING_INVOICE',
        'target_table': 'N_AMEX_TRAVEL_BOOKING_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.832185+00:00'
    }
) }}

WITH 

source_w_amex_travel_booking_invoice AS (
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
    FROM {{ source('raw', 'w_amex_travel_booking_invoice') }}
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
        source_deleted_flg
    FROM source_w_amex_travel_booking_invoice
)

SELECT * FROM final