{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_csone_hb_pc_stech_junction', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_CSONE_HB_PC_STECH_JUNCTION',
        'target_table': 'EL_CSONE_HB_PC_STECH_JUNCTION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.598096+00:00'
    }
) }}

WITH 

source_st_csone_hb_pc_stech_junction AS (
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
    FROM {{ source('raw', 'st_csone_hb_pc_stech_junction') }}
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
        sub_technology_lookup__c,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_csone_hb_pc_stech_junction
)

SELECT * FROM final