{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_srm_cbebre_logs_cselist', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_SRM_CBEBRE_LOGS_CSELIST',
        'target_table': 'ST_TSS_SRM_CBEBRE_LOGS_CSELIST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.598552+00:00'
    }
) }}

WITH 

source_ff_bre_cselist AS (
    SELECT
        transaction_id,
        cseid
    FROM {{ source('raw', 'ff_bre_cselist') }}
),

transformed_exptrans AS (
    SELECT
    transaction_id,
    cseid,
    IFF(LTRIM(RTRIM(TRANSACTION_ID)) = '\N',NULL,TRANSACTION_ID) AS o_transaction_id,
    IFF(LTRIM(RTRIM(CSEID)) = '\N',NULL,CSEID) AS o_cseid
    FROM source_ff_bre_cselist
),

final AS (
    SELECT
        transaction_id,
        cseid
    FROM transformed_exptrans
)

SELECT * FROM final