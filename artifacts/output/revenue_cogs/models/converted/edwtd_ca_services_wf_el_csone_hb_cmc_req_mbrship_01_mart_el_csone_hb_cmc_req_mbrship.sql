{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_csone_hb_cmc_req_mbrship', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_CSONE_HB_CMC_REQ_MBRSHIP',
        'target_table': 'EL_CSONE_HB_CMC_REQ_MBRSHIP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.897477+00:00'
    }
) }}

WITH 

source_st_csone_hb_cmc_req_mbrship AS (
    SELECT
        hb_key,
        c3_sr_number,
        member_user_id,
        createddate,
        casemoncollreqmembership_id,
        casemoncollabrequest_id,
        isdeleted,
        lastmodifieddate,
        membership_role,
        previous_role
    FROM {{ source('raw', 'st_csone_hb_cmc_req_mbrship') }}
),

final AS (
    SELECT
        hb_key,
        c3_sr_number,
        member_user_id,
        createddate,
        casemoncollreqmembership_id,
        casemoncollabrequest_id,
        isdeleted,
        lastmodifieddate,
        membership_role,
        previous_role,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_csone_hb_cmc_req_mbrship
)

SELECT * FROM final