{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_claim_event', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_CLAIM_EVENT',
        'target_table': 'SM_CLAIM_EVENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.986079+00:00'
    }
) }}

WITH 

source_sm_claim_event AS (
    SELECT
        claim_event_key,
        sk_claim_event_id,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_claim_event') }}
),

final AS (
    SELECT
        claim_event_key,
        sk_claim_event_id,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_claim_event
)

SELECT * FROM final