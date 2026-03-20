{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ariba_urap_invoice', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_WI_ARIBA_URAP_INVOICE',
        'target_table': 'WI_ARIBA_URAP_INV_PAID_AMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.654470+00:00'
    }
) }}

WITH 

source_wi_ariba_urap_inv_paid_amt AS (
    SELECT
        po_distribution_id,
        paid_amount
    FROM {{ source('raw', 'wi_ariba_urap_inv_paid_amt') }}
),

source_wi_ariba_urap_inv_ariba_amt AS (
    SELECT
        po_distribution_id,
        ariba_amount
    FROM {{ source('raw', 'wi_ariba_urap_inv_ariba_amt') }}
),

source_wi_ariba_urap_inv_ap_amt AS (
    SELECT
        po_distribution_id,
        ap_amount
    FROM {{ source('raw', 'wi_ariba_urap_inv_ap_amt') }}
),

final AS (
    SELECT
        po_distribution_id,
        paid_amount
    FROM source_wi_ariba_urap_inv_ap_amt
)

SELECT * FROM final