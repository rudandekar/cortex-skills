{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_bre_cr_dispatch', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_BRE_CR_DISPATCH',
        'target_table': 'WI_BRE_CR_DISPATCH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.109520+00:00'
    }
) }}

WITH 

source_wi_bre_cr_dispatch AS (
    SELECT
        bre_trx_log_id,
        creation_date,
        transaction_id,
        eventdatetimec,
        collaborationextidc,
        eventtypec,
        event_yearmonth
    FROM {{ source('raw', 'wi_bre_cr_dispatch') }}
),

final AS (
    SELECT
        bre_trx_log_id,
        creation_date,
        transaction_id,
        eventdatetimec,
        collaborationextidc,
        eventtypec,
        event_yearmonth
    FROM source_wi_bre_cr_dispatch
)

SELECT * FROM final