{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_otm_gct_events', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_OTM_GCT_EVENTS',
        'target_table': 'ST_OTM_GCT_EVENTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.563066+00:00'
    }
) }}

WITH 

source_ff_otm_gct_events AS (
    SELECT
        event_id,
        claim_id,
        created_by,
        created_date,
        event_description,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'ff_otm_gct_events') }}
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
    FROM source_ff_otm_gct_events
)

SELECT * FROM final