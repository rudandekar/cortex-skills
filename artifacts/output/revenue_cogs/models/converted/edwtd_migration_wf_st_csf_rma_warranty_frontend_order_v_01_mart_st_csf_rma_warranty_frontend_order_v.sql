{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csf_rma_warranty_frontend_order_v', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_ST_CSF_RMA_WARRANTY_FRONTEND_ORDER_V',
        'target_table': 'ST_CSF_RMA_WARRANTY_FRONTEND_ORDER_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.779090+00:00'
    }
) }}

WITH 

source_rma_warranty_frontend_order_v1 AS (
    SELECT
        order_number,
        sr_number,
        creation_date,
        warranty,
        edw_create_dtm
    FROM {{ source('raw', 'rma_warranty_frontend_order_v1') }}
),

final AS (
    SELECT
        order_number,
        sr_number,
        creation_date,
        warranty,
        edw_create_dtm
    FROM source_rma_warranty_frontend_order_v1
)

SELECT * FROM final