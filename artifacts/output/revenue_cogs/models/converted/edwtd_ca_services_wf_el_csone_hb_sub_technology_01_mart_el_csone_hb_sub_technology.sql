{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_csone_hb_sub_technology', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_CSONE_HB_SUB_TECHNOLOGY',
        'target_table': 'EL_CSONE_HB_SUB_TECHNOLOGY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.865947+00:00'
    }
) }}

WITH 

source_st_csone_hb_sub_technology AS (
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
    FROM {{ source('raw', 'st_csone_hb_sub_technology') }}
),

transformed_exp_hv_sub_technology1 AS (
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
    FROM source_st_csone_hb_sub_technology
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
    FROM transformed_exp_hv_sub_technology1
)

SELECT * FROM final