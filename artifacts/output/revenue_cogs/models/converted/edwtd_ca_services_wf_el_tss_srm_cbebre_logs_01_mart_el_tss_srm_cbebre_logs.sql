{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_srm_cbebre_logs', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_SRM_CBEBRE_LOGS',
        'target_table': 'EL_TSS_SRM_CBEBRE_LOGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.476436+00:00'
    }
) }}

WITH 

source_st_tss_srm_cbebre_logs AS (
    SELECT
        bre_trx_log_id,
        event_type,
        creation_date,
        bl_last_update_date,
        transaction_id,
        eventdatetimec,
        collaborationextidc,
        eventtypec,
        start_time
    FROM {{ source('raw', 'st_tss_srm_cbebre_logs') }}
),

final AS (
    SELECT
        bre_trx_log_id,
        event_type,
        creation_date,
        bl_last_update_date,
        transaction_id,
        eventdatetimec,
        collaborationextidc,
        eventtypec,
        start_time,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_tss_srm_cbebre_logs
)

SELECT * FROM final