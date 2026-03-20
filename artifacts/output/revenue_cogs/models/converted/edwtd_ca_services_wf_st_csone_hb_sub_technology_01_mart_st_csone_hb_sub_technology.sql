{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csone_hb_sub_technology', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_CSONE_HB_SUB_TECHNOLOGY',
        'target_table': 'ST_CSONE_HB_SUB_TECHNOLOGY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.440620+00:00'
    }
) }}

WITH 

source_ff_csone_hb_sub_technology AS (
    SELECT
        hb_key,
        sub_tech_id_number__c,
        sub_technology_description__c,
        status__c,
        isdeleted,
        created_by__c,
        createddate,
        modified_by__c,
        lastmodifieddate,
        technology__c,
        technology_id__c,
        technology_name__c,
        sub_technology_id__c
    FROM {{ source('raw', 'ff_csone_hb_sub_technology') }}
),

transformed_exptrans AS (
    SELECT
    hb_key,
    sub_technology_id__c,
    sub_tech_id_number__c,
    sub_technology_description__c,
    status__c,
    isdeleted,
    created_by__c,
    createddate,
    modified_by__c,
    lastmodifieddate,
    technology__c,
    technology_id__c,
    technology_name__c,
    IFF(LTRIM(RTRIM(HB_KEY)) = '\N',NULL,LTRIM(RTRIM(HB_KEY))) AS hb_key1,
    IFF(LTRIM(RTRIM(Sub_Technology_Id__c)) = '\N',NULL,LTRIM(RTRIM(Sub_Technology_Id__c))) AS sub_technology_id__c1,
    IFF(LTRIM(RTRIM(SUB_TECH_ID_NUMBER__C)) = '\N',NULL,LTRIM(RTRIM(SUB_TECH_ID_NUMBER__C))) AS sub_tech_id_number__c1,
    IFF(LTRIM(RTRIM(Sub_Technology_Description__c)) = '\N',NULL,LTRIM(RTRIM(Sub_Technology_Description__c))) AS sub_technology_description__c1,
    IFF(LTRIM(RTRIM(Status__c)) = '\N',NULL,LTRIM(RTRIM(Status__c))) AS status__c1,
    IFF(LTRIM(RTRIM(ISDELETED)) = '\N',NULL,LTRIM(RTRIM(ISDELETED))) AS isdeleted1,
    IFF(LTRIM(RTRIM(CREATED_BY__C)) = '\N',NULL,LTRIM(RTRIM(CREATED_BY__C))) AS created_by__c1,
    IFF(LTRIM(RTRIM(CREATEDDATE)) = '\N',NULL,TO_DATE(CREATEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS createddate1,
    IFF(LTRIM(RTRIM(MODIFIED_BY__C)) = '\N',NULL,LTRIM(RTRIM(MODIFIED_BY__C))) AS modified_by__c1,
    IFF(LTRIM(RTRIM(LASTMODIFIEDDATE)) = '\N',NULL,TO_DATE(LASTMODIFIEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS lastmodifieddate1,
    IFF(LTRIM(RTRIM(Technology__c)) = '\N',NULL,LTRIM(RTRIM(Technology__c))) AS technology__c1,
    IFF(LTRIM(RTRIM(Technology_Id__c)) = '\N',NULL,LTRIM(RTRIM(Technology_Id__c))) AS technology_id__c1,
    IFF(LTRIM(RTRIM(Technology_Name__c)) = '\N',NULL,LTRIM(RTRIM(Technology_Name__c))) AS technology_name__c1
    FROM source_ff_csone_hb_sub_technology
),

final AS (
    SELECT
        hb_key,
        sub_technology_id__c,
        sub_tech_id_number__c,
        sub_technology_description__c,
        status__c,
        isdeleted,
        created_by__c,
        createddate,
        modified_by__c,
        lastmodifieddate,
        technology__c,
        technology_id__c,
        technology_name__c
    FROM transformed_exptrans
)

SELECT * FROM final