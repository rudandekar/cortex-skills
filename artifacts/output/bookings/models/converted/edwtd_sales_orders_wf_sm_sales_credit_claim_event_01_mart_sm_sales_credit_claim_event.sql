{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_sales_credit_claim_event', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_SM_SALES_CREDIT_CLAIM_EVENT',
        'target_table': 'SM_SALES_CREDIT_CLAIM_EVENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.134603+00:00'
    }
) }}

WITH 

source_st_otm_gct_events AS (
    SELECT
        event_id,
        claim_id,
        created_by,
        created_date,
        event_description,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_otm_gct_events') }}
),

final AS (
    SELECT
        sales_credit_claim_event_key,
        sk_event_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_st_otm_gct_events
)

SELECT * FROM final