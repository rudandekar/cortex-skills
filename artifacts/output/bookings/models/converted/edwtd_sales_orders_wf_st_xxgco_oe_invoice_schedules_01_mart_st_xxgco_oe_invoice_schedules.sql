{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxgco_oe_invoice_schedules', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_XXGCO_OE_INVOICE_SCHEDULES',
        'target_table': 'ST_XXGCO_OE_INVOICE_SCHEDULES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.895422+00:00'
    }
) }}

WITH 

source_st_xxgco_oe_invoice_schedules AS (
    SELECT
        so_header_id,
        so_line_id,
        bill_schedule_number,
        bill_schedule_id,
        bill_period_start_date,
        bill_period_end_date,
        bill_status_code,
        currency_code,
        bill_amount,
        bill_quantity,
        bill_release_date,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        program_id,
        last_login_id,
        invoice_number,
        invoice_date,
        source_commit_time
    FROM {{ source('raw', 'st_xxgco_oe_invoice_schedules') }}
),

final AS (
    SELECT
        so_header_id,
        so_line_id,
        bill_schedule_number,
        bill_schedule_id,
        bill_period_start_date,
        bill_period_end_date,
        bill_status_code,
        currency_code,
        bill_amount,
        bill_quantity,
        bill_release_date,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date,
        program_id,
        last_login_id,
        invoice_number,
        invoice_date,
        source_commit_time
    FROM source_st_xxgco_oe_invoice_schedules
)

SELECT * FROM final