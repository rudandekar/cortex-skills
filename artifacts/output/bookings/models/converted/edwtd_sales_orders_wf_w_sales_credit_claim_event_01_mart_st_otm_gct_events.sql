{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sales_credit_claim_event', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_W_SALES_CREDIT_CLAIM_EVENT',
        'target_table': 'ST_OTM_GCT_EVENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.934166+00:00'
    }
) }}

WITH 

source_ex_otm_gct_events AS (
    SELECT
        event_id,
        claim_id,
        created_by,
        created_date,
        event_description,
        exception_type,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'ex_otm_gct_events') }}
),

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
        event_id,
        claim_id,
        created_by,
        created_date,
        event_description,
        batch_id,
        create_datetime
    FROM source_st_otm_gct_events
)

SELECT * FROM final