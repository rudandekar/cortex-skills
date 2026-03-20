{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_csone_hb_technology', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_CSONE_HB_TECHNOLOGY',
        'target_table': 'EL_CSONE_HB_TECHNOLOGY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.804963+00:00'
    }
) }}

WITH 

source_st_csone_hb_technology AS (
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
    FROM {{ source('raw', 'st_csone_hb_technology') }}
),

transformed_exp_hv_technology AS (
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
    FROM source_st_csone_hb_technology
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
    FROM transformed_exp_hv_technology
)

SELECT * FROM final