{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_rma_warranty_frontend_order', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_RMA_WARRANTY_FRONTEND_ORDER',
        'target_table': 'ST_RMA_WARRANTY_FRONTEND_ORDER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.183671+00:00'
    }
) }}

WITH 

source_ff_rma_warranty_frontend_order_v AS (
    SELECT
        order_number,
        sr_number,
        fiscal_month_name,
        fiscal_quarter_name,
        warranty,
        fiscal_month_id
    FROM {{ source('raw', 'ff_rma_warranty_frontend_order_v') }}
),

final AS (
    SELECT
        order_number,
        sr_number,
        fiscal_month_name,
        fiscal_quarter_name,
        warranty,
        fiscal_month_id
    FROM source_ff_rma_warranty_frontend_order_v
)

SELECT * FROM final