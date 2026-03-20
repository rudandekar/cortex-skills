{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_bre_cselist', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_BRE_CSELIST',
        'target_table': 'WI_BRE_CSELIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.164251+00:00'
    }
) }}

WITH 

source_wi_bre_cselist AS (
    SELECT
        transaction_id,
        eventtypec,
        cseid
    FROM {{ source('raw', 'wi_bre_cselist') }}
),

final AS (
    SELECT
        transaction_id,
        eventtypec,
        cseid
    FROM source_wi_bre_cselist
)

SELECT * FROM final