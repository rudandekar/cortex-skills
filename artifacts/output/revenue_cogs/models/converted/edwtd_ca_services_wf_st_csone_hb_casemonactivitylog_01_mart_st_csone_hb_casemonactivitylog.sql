{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csone_casemonactivitylog', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_CSONE_CASEMONACTIVITYLOG',
        'target_table': 'ST_CSONE_HB_CASEMONACTIVITYLOG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.538388+00:00'
    }
) }}

WITH 

source_ff_csone_hb_casemonactivitylog AS (
    SELECT
        hb_key,
        casemoncollaborationrequest_id,
        owner_cec_id,
        createddate,
        activity,
        id,
        lastmodifieddate
    FROM {{ source('raw', 'ff_csone_hb_casemonactivitylog') }}
),

transformed_expr AS (
    SELECT
    hb_key,
    casemoncollaborationrequest_id,
    owner_cec_id,
    createddate,
    activity,
    casemonactivitylog_id,
    lastmodifieddate,
    IFF(LTRIM(RTRIM(HB_KEY)) = '\N',NULL,HB_KEY) AS o_hb_key,
    IFF(LTRIM(RTRIM(CASEMONCOLLABORATIONREQUEST_ID)) = '\N',NULL,CASEMONCOLLABORATIONREQUEST_ID) AS o_casemoncollaborationrequest_id,
    IFF(LTRIM(RTRIM(OWNER_CEC_ID)) = '\N',NULL,OWNER_CEC_ID) AS o_owner_cec_id,
    IFF(LTRIM(RTRIM(CREATEDDATE)) = '\N',NULL,TO_DATE(CREATEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS o_createddate,
    IFF(LTRIM(RTRIM(ACTIVITY)) = '\N',NULL,ACTIVITY) AS o_activity,
    IFF(LTRIM(RTRIM(CASEMONACTIVITYLOG_ID)) = '\N',NULL,CASEMONACTIVITYLOG_ID) AS o_casemonactivitylog_id,
    IFF(LTRIM(RTRIM(LASTMODIFIEDDATE)) = '\N',NULL,TO_DATE(LASTMODIFIEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS o_lastmodifieddate
    FROM source_ff_csone_hb_casemonactivitylog
),

final AS (
    SELECT
        hb_key,
        casemoncollaborationrequest_id,
        owner_cec_id,
        createddate,
        activity,
        casemonactivitylog_id,
        lastmodifieddate
    FROM transformed_expr
)

SELECT * FROM final