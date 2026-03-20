{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_rma_warranty_backend_v', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_RMA_WARRANTY_BACKEND_V',
        'target_table': 'ST_RMA_WARRANTY_BACKEND_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.801749+00:00'
    }
) }}

WITH 

source_ff_rma_warranty_backend_v AS (
    SELECT
        order_number,
        sr_number,
        received_pid,
        fiscal_month_name,
        fiscal_quarter_name,
        parts_returned,
        parts_on_contract,
        parts_on_warranty,
        warranty_rma,
        fiscal_month_id
    FROM {{ source('raw', 'ff_rma_warranty_backend_v') }}
),

final AS (
    SELECT
        order_number,
        sr_number,
        received_pid,
        fiscal_month_name,
        fiscal_quarter_name,
        parts_returned,
        parts_on_contract,
        parts_on_warranty,
        warranty_rma,
        fiscal_month_id
    FROM source_ff_rma_warranty_backend_v
)

SELECT * FROM final