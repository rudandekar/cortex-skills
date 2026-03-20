{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csf_rcv_shipment_lines', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_ST_CSF_RCV_SHIPMENT_LINES',
        'target_table': 'ST_CSF_RCV_SHIPMENT_LINES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.219113+00:00'
    }
) }}

WITH 

source_ff_csf_rcv_shipment_lines AS (
    SELECT
        shipment_line_id,
        shipment_header_id,
        oe_order_header_id,
        oe_order_line_id,
        creation_date,
        last_update_date,
        edw_create_dtm
    FROM {{ source('raw', 'ff_csf_rcv_shipment_lines') }}
),

source_rcv_shipment_lines AS (
    SELECT
        shipment_line_id,
        shipment_header_id,
        oe_order_header_id,
        oe_order_line_id,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'rcv_shipment_lines') }}
),

final AS (
    SELECT
        shipment_line_id,
        shipment_header_id,
        oe_order_header_id,
        oe_order_line_id,
        creation_date,
        last_update_date,
        edw_create_dtm
    FROM source_rcv_shipment_lines
)

SELECT * FROM final