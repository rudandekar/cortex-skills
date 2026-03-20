{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_csone_hb_casemonactivitylog', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_CSONE_HB_CASEMONACTIVITYLOG',
        'target_table': 'EL_CSONE_HB_CASEMONACTIVITYLOG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.001579+00:00'
    }
) }}

WITH 

source_st_csone_hb_casemonactivitylog AS (
    SELECT
        hb_key,
        casemoncollaborationrequest_id,
        created_by,
        createddate,
        activity,
        id,
        isdeleted,
        lastmodifieddate
    FROM {{ source('raw', 'st_csone_hb_casemonactivitylog') }}
),

final AS (
    SELECT
        hb_key,
        casemoncollaborationrequest_id,
        owner_cec_id,
        createddate,
        activity,
        casemonactivitylog_id,
        lastmodifieddate,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_csone_hb_casemonactivitylog
)

SELECT * FROM final