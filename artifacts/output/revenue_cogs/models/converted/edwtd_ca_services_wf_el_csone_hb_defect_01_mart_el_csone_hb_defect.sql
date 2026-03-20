{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_csone_hb_defect', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_CSONE_HB_DEFECT',
        'target_table': 'EL_CSONE_HB_DEFECT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.966386+00:00'
    }
) }}

WITH 

source_st_csone_hb_defect AS (
    SELECT
        hb_key,
        case_c3number__c,
        case_casenumber__c,
        createdbyid,
        createddate,
        created_by__c,
        defect__c,
        id,
        isdeleted,
        lastmodifiedbyid,
        lastmodifieddate,
        lastmodified_c3id__c,
        link__c,
        modified_by__c,
        name_1,
        resend_message__c
    FROM {{ source('raw', 'st_csone_hb_defect') }}
),

final AS (
    SELECT
        hb_key,
        case_c3number__c,
        case_casenumber__c,
        createdbyid,
        createddate,
        created_by__c,
        defect__c,
        id,
        isdeleted,
        lastmodifiedbyid,
        lastmodifieddate,
        lastmodified_c3id__c,
        link__c,
        modified_by__c,
        name_1,
        resend_message__c,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_csone_hb_defect
)

SELECT * FROM final