{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxopl_billing_schedule', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_XXOPL_BILLING_SCHEDULE',
        'target_table': 'ST_XXOPL_BILLING_SCHEDULE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.238970+00:00'
    }
) }}

WITH 

source_st_si_xxopl_billing_schedule AS (
    SELECT
        bill_schedule_number,
        bill_schedule_id,
        bill_period_end_date,
        bill_period_start_date,
        bill_status,
        currency_code,
        bill_amount,
        bill_release_date,
        bill_quantity,
        invoice_amount,
        invoice_date,
        invoice_number,
        header_id,
        line_id,
        line_ref_number,
        order_origin,
        record_offset,
        edw_create_dtm
    FROM {{ source('raw', 'st_si_xxopl_billing_schedule') }}
),

final AS (
    SELECT
        bill_schedule_number,
        bill_schedule_id,
        bill_period_end_date,
        bill_period_start_date,
        bill_status,
        currency_code,
        bill_amount,
        bill_release_date,
        bill_quantity,
        invoice_amount,
        invoice_date,
        invoice_number,
        header_id,
        line_id,
        line_ref_number,
        order_origin,
        record_offset,
        edw_create_dtm
    FROM source_st_si_xxopl_billing_schedule
)

SELECT * FROM final