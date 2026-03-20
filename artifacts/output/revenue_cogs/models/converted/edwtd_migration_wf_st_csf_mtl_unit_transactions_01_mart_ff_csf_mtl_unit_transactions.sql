{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_csf_mtl_unit_transactions', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_FF_CSF_MTL_UNIT_TRANSACTIONS',
        'target_table': 'FF_CSF_MTL_UNIT_TRANSACTIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.480433+00:00'
    }
) }}

WITH 

source_csf_mtl_unit_transactions AS (
    SELECT
        rownumber,
        transaction_id,
        serial_number,
        last_update_date,
        creation_date
    FROM {{ source('raw', 'csf_mtl_unit_transactions') }}
),

source_ff_csf_mtl_unit_transactions AS (
    SELECT
        rownumber,
        transaction_id,
        serial_number,
        last_update_date,
        creation_date,
        edw_create_dtm
    FROM {{ source('raw', 'ff_csf_mtl_unit_transactions') }}
),

final AS (
    SELECT
        rownumber,
        transaction_id,
        serial_number,
        last_update_date,
        creation_date,
        edw_create_dtm
    FROM source_ff_csf_mtl_unit_transactions
)

SELECT * FROM final