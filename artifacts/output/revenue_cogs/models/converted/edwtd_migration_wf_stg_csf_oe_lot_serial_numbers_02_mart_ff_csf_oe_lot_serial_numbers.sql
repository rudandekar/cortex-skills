{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_stg_csf_oe_lot_serial_numbers', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_STG_CSF_OE_LOT_SERIAL_NUMBERS',
        'target_table': 'FF_CSF_OE_LOT_SERIAL_NUMBERS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.034587+00:00'
    }
) }}

WITH 

source_ff_csf_oe_lot_serial_numbers AS (
    SELECT
        header_id,
        line_id,
        customer_provided_sn
    FROM {{ source('raw', 'ff_csf_oe_lot_serial_numbers') }}
),

source_oe_order_headers_all AS (
    SELECT
        header_id,
        line_id,
        customer_provided_sn
    FROM {{ source('raw', 'oe_order_headers_all') }}
),

final AS (
    SELECT
        header_id,
        line_id,
        customer_provided_sn
    FROM source_oe_order_headers_all
)

SELECT * FROM final