{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_csf_rma_warranty_backend_v', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_EL_CSF_RMA_WARRANTY_BACKEND_V',
        'target_table': 'EL_CSF_RMA_WARRANTY_BACKEND_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.760534+00:00'
    }
) }}

WITH 

source_st_rma_warranty_backend_v AS (
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
    FROM {{ source('raw', 'st_rma_warranty_backend_v') }}
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
    FROM source_st_rma_warranty_backend_v
)

SELECT * FROM final