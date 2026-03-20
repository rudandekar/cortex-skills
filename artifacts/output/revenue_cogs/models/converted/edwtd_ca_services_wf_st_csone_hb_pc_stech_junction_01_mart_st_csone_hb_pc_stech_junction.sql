{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csone_hb_pc_stech_junction', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_CSONE_HB_PC_STECH_JUNCTION',
        'target_table': 'ST_CSONE_HB_PC_STECH_JUNCTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.895707+00:00'
    }
) }}

WITH 

source_ff_csone_hb_pc_stech_junction AS (
    SELECT
        hb_key,
        createddate,
        external__c,
        id,
        isdeleted,
        lastmodifieddate,
        problem_code_lookup__c,
        problem_code__c,
        status__c,
        sub_technology_id__c,
        sub_technology_lookup__c
    FROM {{ source('raw', 'ff_csone_hb_pc_stech_junction') }}
),

transformed_expr AS (
    SELECT
    hb_key,
    createddate,
    external__c,
    id,
    isdeleted,
    lastmodifieddate,
    problem_code_lookup__c,
    problem_code__c,
    status__c,
    sub_technology_id__c,
    sub_technology_lookup__c,
    IFF(LTRIM(RTRIM(HB_KEY)) = '\N',NULL,HB_KEY) AS o_hb_key,
    IFF(LTRIM(RTRIM(CREATEDDATE)) = '\N',NULL,TO_DATE(CREATEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS o_createddate,
    IFF(LTRIM(RTRIM(EXTERNAL__C)) = '\N',NULL,EXTERNAL__C) AS o_external__c,
    IFF(LTRIM(RTRIM(ID)) = '\N',NULL,ID) AS o_id,
    IFF(LTRIM(RTRIM(ISDELETED)) = '\N',NULL,ISDELETED) AS o_isdeleted,
    IFF(LTRIM(RTRIM(LASTMODIFIEDDATE)) = '\N',NULL,TO_DATE(LASTMODIFIEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS o_lastmodifieddate,
    IFF(LTRIM(RTRIM(PROBLEM_CODE_LOOKUP__C)) = '\N',NULL,PROBLEM_CODE_LOOKUP__C) AS o_problem_code_lookup__c,
    IFF(LTRIM(RTRIM(PROBLEM_CODE__C)) = '\N',NULL,PROBLEM_CODE__C) AS o_problem_code__c,
    IFF(LTRIM(RTRIM(STATUS__C)) = '\N',NULL,STATUS__C) AS o_status__c,
    IFF(LTRIM(RTRIM(SUB_TECHNOLOGY_ID__C)) = '\N',NULL,SUB_TECHNOLOGY_ID__C) AS o_sub_technology_id__c,
    IFF(LTRIM(RTRIM(SUB_TECHNOLOGY_LOOKUP__C)) = '\N',NULL,SUB_TECHNOLOGY_LOOKUP__C) AS o_sub_technology_lookup__c
    FROM source_ff_csone_hb_pc_stech_junction
),

final AS (
    SELECT
        hb_key,
        createddate,
        external__c,
        id,
        isdeleted,
        lastmodifieddate,
        problem_code_lookup__c,
        problem_code__c,
        status__c,
        sub_technology_id__c,
        sub_technology_lookup__c
    FROM transformed_expr
)

SELECT * FROM final