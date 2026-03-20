{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_rma_warranty_frontend_order', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_RMA_WARRANTY_FRONTEND_ORDER',
        'target_table': 'EL_RMA_WARRANTY_FRONTEND_ORDER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.276071+00:00'
    }
) }}

WITH 

source_st_rma_warranty_frontend_order AS (
    SELECT
        order_number,
        sr_number,
        fiscal_month_name,
        fiscal_quarter_name,
        warranty,
        fiscal_month_id
    FROM {{ source('raw', 'st_rma_warranty_frontend_order') }}
),

final AS (
    SELECT
        order_number,
        sr_number,
        fiscal_month_name,
        fiscal_quarter_name,
        warranty,
        fiscal_month_id
    FROM source_st_rma_warranty_frontend_order
)

SELECT * FROM final