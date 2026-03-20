{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_claim_end_cust', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_CLAIM_END_CUST',
        'target_table': 'SM_CLAIM_END_CUST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.969979+00:00'
    }
) }}

WITH 

source_sm_claim_end_cust AS (
    SELECT
        claim_end_cust_key,
        sk_src_sav_detail_id,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_claim_end_cust') }}
),

final AS (
    SELECT
        claim_end_cust_key,
        sk_src_sav_detail_id,
        edw_create_dtm,
        edw_create_user
    FROM source_sm_claim_end_cust
)

SELECT * FROM final