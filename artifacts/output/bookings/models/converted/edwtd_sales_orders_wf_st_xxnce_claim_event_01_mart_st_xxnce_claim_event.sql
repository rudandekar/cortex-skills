{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxnce_claim_event', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_XXNCE_CLAIM_EVENT',
        'target_table': 'ST_XXNCE_CLAIM_EVENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.847670+00:00'
    }
) }}

WITH 

source_xxnce_claim_event AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        event_id,
        event_type,
        creation_date,
        created_by,
        last_updated_by,
        last_updated_date,
        object_version_number,
        claim_id,
        pending_action_actor,
        actor_type,
        claim_trx_sc_id,
        claim_status,
        trxn_approval_status
    FROM {{ source('raw', 'xxnce_claim_event') }}
),

final AS (
    SELECT
        event_id,
        event_type,
        creation_date,
        created_by,
        last_updated_by,
        last_updated_date,
        object_version_number,
        claim_id,
        pending_action_actor,
        actor_type,
        claim_trx_sc_id,
        claim_status,
        trxn_approval_status
    FROM source_xxnce_claim_event
)

SELECT * FROM final