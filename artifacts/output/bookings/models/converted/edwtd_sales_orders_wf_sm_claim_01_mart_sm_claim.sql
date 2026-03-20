{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_claim', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_CLAIM',
        'target_table': 'SM_CLAIM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.459512+00:00'
    }
) }}

WITH 

source_sm_claim AS (
    SELECT
        claim_key,
        sk_claim_id,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_claim') }}
),

final AS (
    SELECT
        claim_key,
        sk_claim_id,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_claim
)

SELECT * FROM final