{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_csone_hb_casemoncollrequest', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_CSONE_HB_CASEMONCOLLREQUEST',
        'target_table': 'EL_CSONE_HB_CASEMONCOLLREQUEST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.450097+00:00'
    }
) }}

WITH 

source_st_csone_hb_casemoncollrequest AS (
    SELECT
        hb_key,
        activity_date,
        c3_sr_id,
        cancelleddate,
        accepteddate,
        acceptedby,
        collbr_requester,
        createddate,
        cancelledby,
        cse_cec_user_id,
        c3_sr_number,
        collaboration_request_ext_id,
        collbr_type,
        id,
        isdeleted,
        status,
        collbr_sub_type,
        collaborationreason,
        collab_initiator_proficiency,
        case_tech_id,
        case_subtech_id,
        case_probcode_lookup,
        collbr_tech_id,
        collbr_subtech_id,
        collbr_probcode_lookup,
        case_transfer_reason,
        collbr_request_desc,
        name_val,
        timezone_val,
        lastmodifieddate
    FROM {{ source('raw', 'st_csone_hb_casemoncollrequest') }}
),

final AS (
    SELECT
        hb_key,
        activity_date,
        c3_sr_id,
        cancelleddate,
        accepteddate,
        acceptedby,
        collbr_requester,
        createddate,
        cancelledby,
        cse_cec_user_id,
        c3_sr_number,
        collaboration_request_ext_id,
        collbr_type,
        id,
        isdeleted,
        status,
        collbr_sub_type,
        collaborationreason,
        collab_initiator_proficiency,
        case_tech_id,
        case_subtech_id,
        case_probcode_lookup,
        collbr_tech_id,
        collbr_subtech_id,
        collbr_probcode_lookup,
        case_transfer_reason,
        collbr_request_desc,
        name_val,
        timezone_val,
        lastmodifieddate,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_csone_hb_casemoncollrequest
)

SELECT * FROM final