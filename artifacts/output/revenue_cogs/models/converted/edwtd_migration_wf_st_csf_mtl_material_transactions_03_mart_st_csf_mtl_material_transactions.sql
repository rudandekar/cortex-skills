{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_csf_mtl_material_transactions ', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_FF_CSF_MTL_MATERIAL_TRANSACTIONS ',
        'target_table': 'ST_CSF_MTL_MATERIAL_TRANSACTIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.580319+00:00'
    }
) }}

WITH 

source_ff_csf_mtl_material_transactions AS (
    SELECT
        transaction_id,
        rcv_transaction_id,
        creation_date,
        last_update_date,
        edw_create_dtm,
        source_line_id
    FROM {{ source('raw', 'ff_csf_mtl_material_transactions') }}
),

source_mtl_material_transactions AS (
    SELECT
        transaction_id,
        rcv_transaction_id,
        creation_date,
        last_update_date,
        source_line_id
    FROM {{ source('raw', 'mtl_material_transactions') }}
),

final AS (
    SELECT
        transaction_id,
        rcv_transaction_id,
        creation_date,
        last_update_date,
        edw_create_dtm,
        source_line_id
    FROM source_mtl_material_transactions
)

SELECT * FROM final