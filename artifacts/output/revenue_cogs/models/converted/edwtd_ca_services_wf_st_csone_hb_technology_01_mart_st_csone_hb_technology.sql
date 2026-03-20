{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csone_hb_technology', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_CSONE_HB_TECHNOLOGY',
        'target_table': 'ST_CSONE_HB_TECHNOLOGY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.474728+00:00'
    }
) }}

WITH 

source_ff_csone_hb_technology AS (
    SELECT
        hb_key,
        technology_id__c,
        technology_description__c,
        status__c,
        isdeleted,
        created_by__c,
        createddate,
        modified_by__c,
        lastmodifieddate
    FROM {{ source('raw', 'ff_csone_hb_technology') }}
),

transformed_exp_hb_technology AS (
    SELECT
    hb_key,
    technology_id__c,
    technology_description__c,
    status__c,
    isdeleted,
    created_by__c,
    createddate,
    modified_by__c,
    lastmodifieddate,
    IFF(LTRIM(RTRIM(HB_KEY)) = '\N',NULL,LTRIM(RTRIM(HB_KEY))) AS o_hb_key,
    IFF(LTRIM(RTRIM(Technology_Id__c)) = '\N',NULL,LTRIM(RTRIM(Technology_Id__c))) AS technology_id__c1,
    IFF(LTRIM(RTRIM(Technology_Description__c)) = '\N',NULL,LTRIM(RTRIM(Technology_Description__c))) AS technology_description__c1,
    IFF(LTRIM(RTRIM(Status__c)) = '\N',NULL,LTRIM(RTRIM(Status__c))) AS status__c1,
    IFF(LTRIM(RTRIM(ISDELETED)) = '\N',NULL,LTRIM(RTRIM(ISDELETED))) AS isdeleted1,
    IFF(LTRIM(RTRIM(CREATED_BY__C)) = '\N',NULL,LTRIM(RTRIM(CREATED_BY__C))) AS created_by__c1,
    IFF(LTRIM(RTRIM(CREATEDDATE)) = '\N',NULL,TO_DATE(CREATEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS createddate1,
    IFF(LTRIM(RTRIM(MODIFIED_BY__C)) = '\N',NULL,LTRIM(RTRIM(MODIFIED_BY__C))) AS modified_by__c1,
    IFF(LTRIM(RTRIM(LASTMODIFIEDDATE)) = '\N',NULL,TO_DATE(LASTMODIFIEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS lastmodifieddate1
    FROM source_ff_csone_hb_technology
),

final AS (
    SELECT
        hb_key,
        technology_id__c,
        technology_description__c,
        status__c,
        isdeleted,
        created_by__c,
        createddate,
        modified_by__c,
        lastmodifieddate
    FROM transformed_exp_hb_technology
)

SELECT * FROM final