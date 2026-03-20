{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csone_hb_defect', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_CSONE_HB_DEFECT',
        'target_table': 'ST_CSONE_HB_DEFECT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.229130+00:00'
    }
) }}

WITH 

source_ff_csone_hb_defect AS (
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
    FROM {{ source('raw', 'ff_csone_hb_defect') }}
),

transformed_expr AS (
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
    IFF(LTRIM(RTRIM(HB_KEY)) = '\N',NULL,HB_KEY) AS o_hb_key,
    IFF(LTRIM(RTRIM(CASE_C3NUMBER__C)) = '\N',NULL,CASE_C3NUMBER__C) AS o_case_c3number__c,
    IFF(LTRIM(RTRIM(CASE_CASENUMBER__C)) = '\N',NULL,CASE_CASENUMBER__C) AS o_case_casenumber__c,
    IFF(LTRIM(RTRIM(CREATEDBYID)) = '\N',NULL,CREATEDBYID) AS o_createdbyid,
    IFF(LTRIM(RTRIM(CREATEDDATE)) = '\N',NULL,TO_DATE(CREATEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS o_createddate,
    IFF(LTRIM(RTRIM(CREATED_BY__C)) = '\N',NULL,CREATED_BY__C) AS o_created_by__c,
    IFF(LTRIM(RTRIM(DEFECT__C)) = '\N',NULL,DEFECT__C) AS o_defect__c,
    IFF(LTRIM(RTRIM(ID)) = '\N',NULL,ID) AS o_id,
    IFF(LTRIM(RTRIM(ISDELETED)) = '\N',NULL,ISDELETED) AS o_isdeleted,
    IFF(LTRIM(RTRIM(LASTMODIFIEDBYID)) = '\N',NULL,LASTMODIFIEDBYID) AS o_lastmodifiedbyid,
    IFF(LTRIM(RTRIM(LASTMODIFIEDDATE)) = '\N',NULL,TO_DATE(LASTMODIFIEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS o_lastmodifieddate,
    IFF(LTRIM(RTRIM(LASTMODIFIED_C3ID__C)) = '\N',NULL,LASTMODIFIED_C3ID__C) AS o_lastmodified_c3id__c,
    IFF(LTRIM(RTRIM(LINK__C)) = '\N',NULL,LINK__C) AS o_link__c,
    IFF(LTRIM(RTRIM(MODIFIED_BY__C)) = '\N',NULL,MODIFIED_BY__C) AS o_modified_by__c,
    IFF(LTRIM(RTRIM(NAME_1)) = '\N',NULL,NAME_1) AS o_name_1,
    IFF(LTRIM(RTRIM(RESEND_MESSAGE__C)) = '\N',NULL,RESEND_MESSAGE__C) AS o_resend_message__c
    FROM source_ff_csone_hb_defect
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
        resend_message__c
    FROM transformed_expr
)

SELECT * FROM final