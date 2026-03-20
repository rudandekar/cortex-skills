{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csf_rma_warranty_frontend_v', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_ST_CSF_RMA_WARRANTY_FRONTEND_V',
        'target_table': 'ST_CSF_RMA_WARRANTY_FRONTEND_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.619197+00:00'
    }
) }}

WITH 

source_rma_warranty_frontend_v1 AS (
    SELECT
        order_number,
        sr_number,
        ordered_item,
        creation_date,
        warranty,
        edw_create_dtm
    FROM {{ source('raw', 'rma_warranty_frontend_v1') }}
),

final AS (
    SELECT
        order_number,
        sr_number,
        ordered_item,
        creation_date,
        warranty,
        edw_create_dtm
    FROM source_rma_warranty_frontend_v1
)

SELECT * FROM final