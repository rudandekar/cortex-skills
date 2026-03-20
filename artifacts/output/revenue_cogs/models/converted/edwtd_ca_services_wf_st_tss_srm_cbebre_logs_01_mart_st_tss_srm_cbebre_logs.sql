{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_tss_srm_cbebre_logs', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_TSS_SRM_CBEBRE_LOGS',
        'target_table': 'ST_TSS_SRM_CBEBRE_LOGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.723949+00:00'
    }
) }}

WITH 

source_ff_tss_srm_cbebre_logs AS (
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
    FROM {{ source('raw', 'ff_tss_srm_cbebre_logs') }}
),

transformed_ex_st_tss_srm_cbebre_logs AS (
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
    IFF(LTRIM(RTRIM(BRE_TRX_LOG_ID))='\N',-999,TO_BIGINT(BRE_TRX_LOG_ID)) AS o_bre_trx_log_id,
    IFF(LTRIM(RTRIM(EVENT_TYPE)) = '\N',NULL,EVENT_TYPE) AS o_event_type,
    IFF(LTRIM(RTRIM(CREATION_DATE)) = '\N',NULL,TO_DATE(CREATION_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_creation_date,
    IFF(LTRIM(RTRIM(BL_LAST_UPDATE_DATE)) = '\N',TO_DATE('3500-01-01 00:00:00','YYYY-MM-DD HH24:MI:SS'),TO_DATE(BL_LAST_UPDATE_DATE,'YYYY-MM-DD HH24:MI:SS')) AS o_bl_last_update_date,
    IFF(LTRIM(RTRIM(TRANSACTION_ID)) = '\N',NULL,TRANSACTION_ID) AS o_transaction_id,
    IFF(LTRIM(RTRIM(EVENTDATETIMEC)) = '\N',NULL,TO_DATE(EVENTDATETIMEC,'YYYY-MM-DD HH24:MI:SS')) AS o_eventdatetimec,
    IFF(LTRIM(RTRIM(COLLABORATIONEXTIDC)) = '\N',NULL,COLLABORATIONEXTIDC) AS o_collaborationextidc,
    IFF(LTRIM(RTRIM(EVENTTYPEC)) = '\N',NULL,EVENTTYPEC) AS o_eventtypec,
    IFF(LTRIM(RTRIM(START_TIME))='\N',NULL,IFF(LENGTH(START_TIME)>19,TO_DATE(SUBSTR(START_TIME , 0,INSTR(START_TIME,'.')-1),'YYYY-MM-DD HH24:MI:SS'),TO_DATE(START_TIME,'YYYY-MM-DD HH24:MI:SS'))) AS o_start_time
    FROM source_ff_tss_srm_cbebre_logs
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
        start_time
    FROM transformed_ex_st_tss_srm_cbebre_logs
)

SELECT * FROM final