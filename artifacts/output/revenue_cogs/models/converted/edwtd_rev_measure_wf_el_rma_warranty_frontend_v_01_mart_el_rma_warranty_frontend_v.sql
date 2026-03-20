{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_rma_warranty_frontend_v', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_RMA_WARRANTY_FRONTEND_V',
        'target_table': 'EL_RMA_WARRANTY_FRONTEND_V',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.512921+00:00'
    }
) }}

WITH 

source_st_rma_warranty_frontend_v AS (
    SELECT
        order_number,
        sr_number,
        ordered_item,
        fiscal_month_name,
        fiscal_quarter_name,
        warranty,
        fiscal_month_id
    FROM {{ source('raw', 'st_rma_warranty_frontend_v') }}
),

final AS (
    SELECT
        order_number,
        sr_number,
        ordered_item,
        fiscal_month_name,
        fiscal_quarter_name,
        warranty,
        fiscal_month_id
    FROM source_st_rma_warranty_frontend_v
)

SELECT * FROM final