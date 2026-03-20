{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_claim_losing_agent', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_CLAIM_LOSING_AGENT',
        'target_table': 'SM_CLAIM_LOSING_AGENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.853828+00:00'
    }
) }}

WITH 

source_sm_claim_losing_agent AS (
    SELECT
        claim_losing_agent_key,
        sk_trx_src_id,
        src_trx_id_int,
        trx_src_type_cd,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_claim_losing_agent') }}
),

final AS (
    SELECT
        claim_losing_agent_key,
        sk_trx_src_id,
        src_trx_id_int,
        trx_src_type_cd,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_claim_losing_agent
)

SELECT * FROM final