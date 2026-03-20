{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csf_rcv_transactions', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_ST_CSF_RCV_TRANSACTIONS',
        'target_table': 'FF_CSF_RCV_TRANSACTIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.222360+00:00'
    }
) }}

WITH 

source_ff_csf_rcv_transactions AS (
    SELECT
        transaction_id,
        shipment_header_id,
        shipment_line_id,
        creation_date,
        last_update_date,
        edw_create_dtm
    FROM {{ source('raw', 'ff_csf_rcv_transactions') }}
),

source_rcv_transactions AS (
    SELECT
        transaction_id,
        shipment_header_id,
        shipment_line_id,
        creation_date,
        last_update_date
    FROM {{ source('raw', 'rcv_transactions') }}
),

final AS (
    SELECT
        transaction_id,
        shipment_header_id,
        shipment_line_id,
        creation_date,
        last_update_date,
        edw_create_dtm
    FROM source_rcv_transactions
)

SELECT * FROM final