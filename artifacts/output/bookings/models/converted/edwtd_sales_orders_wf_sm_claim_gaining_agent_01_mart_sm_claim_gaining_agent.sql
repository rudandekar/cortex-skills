{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_claim_gaining_agent', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_CLAIM_GAINING_AGENT',
        'target_table': 'SM_CLAIM_GAINING_AGENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.578378+00:00'
    }
) }}

WITH 

source_sm_claim_gaining_agent AS (
    SELECT
        claim_gaining_agent_key,
        sk_claim_gaining_agent_id,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_claim_gaining_agent') }}
),

final AS (
    SELECT
        claim_gaining_agent_key,
        sk_claim_gaining_agent_id,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_claim_gaining_agent
)

SELECT * FROM final