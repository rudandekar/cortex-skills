{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_booked_ticket', 'batch', 'edwtd_travel'],
    meta={
        'source_workflow': 'wf_m_EL_BOOKED_TICKET',
        'target_table': 'EL_BOOKED_TICKET',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.861025+00:00'
    }
) }}

WITH 

source_st_booked_ticket AS (
    SELECT
        batch_id,
        xpk_row5,
        fk_travel5,
        reason_code2,
        pax_sequence5,
        compare_fare2,
        compare_fare,
        validating_carrier,
        pnr_locator5,
        customer_id5,
        gds_code5,
        invoice_number,
        gross_fare,
        ticket_number0,
        ticket_type,
        reason_code3,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_booked_ticket') }}
),

final AS (
    SELECT
        xpk_row5,
        fk_travel5,
        reason_code2,
        pax_sequence5,
        compare_fare2,
        compare_fare,
        validating_carrier,
        pnr_locator5,
        customer_id5,
        gds_code5,
        invoice_number,
        gross_fare,
        ticket_number0,
        ticket_type,
        reason_code3,
        deleted_flag,
        create_datetime,
        update_datetime
    FROM source_st_booked_ticket
)

SELECT * FROM final