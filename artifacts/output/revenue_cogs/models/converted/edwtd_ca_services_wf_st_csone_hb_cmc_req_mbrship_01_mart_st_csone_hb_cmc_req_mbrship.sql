{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_csone_hb_cmc_req_mbrship', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_ST_CSONE_HB_CMC_REQ_MBRSHIP',
        'target_table': 'ST_CSONE_HB_CMC_REQ_MBRSHIP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.082691+00:00'
    }
) }}

WITH 

source_ff_csone_hb_cmc_req_mbrship AS (
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
        previousrole
    FROM {{ source('raw', 'ff_csone_hb_cmc_req_mbrship') }}
),

transformed_expr AS (
    SELECT
    hb_key,
    c3_sr_number,
    member_user_id,
    createddate,
    casemoncollreqmembership_id,
    isdeleted,
    lastmodifieddate,
    membership_role,
    previousrole,
    casemoncollabrequest_id,
    IFF(LTRIM(RTRIM(HB_KEY)) = '\N',NULL,HB_KEY) AS o_hb_key,
    IFF(LTRIM(RTRIM(C3_SR_NUMBER)) = '\N',NULL,C3_SR_NUMBER) AS o_c3_sr_number,
    IFF(LTRIM(RTRIM(MEMBER_USER_ID)) = '\N',NULL,MEMBER_USER_ID) AS o_member_user_id,
    IFF(LTRIM(RTRIM(CREATEDDATE)) = '\N',NULL,TO_DATE(CREATEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS o_createddate,
    IFF(LTRIM(RTRIM(CASEMONCOLLREQMEMBERSHIP_ID)) = '\N',NULL,CASEMONCOLLREQMEMBERSHIP_ID) AS o_casemoncollreqmembership_id,
    IFF(LTRIM(RTRIM(ISDELETED)) = '\N',NULL,ISDELETED) AS o_isdeleted,
    IFF(LTRIM(RTRIM(LASTMODIFIEDDATE)) = '\N',NULL,TO_DATE(LASTMODIFIEDDATE,'YYYY-MM-DD HH24:MI:SS')) AS o_lastmodifieddate,
    IFF(LTRIM(RTRIM(MEMBERSHIP_ROLE)) = '\N',NULL,MEMBERSHIP_ROLE) AS o_membership_role,
    IFF(LTRIM(RTRIM(PREVIOUSROLE)) = '\N',NULL,PREVIOUSROLE) AS o_previousrole,
    IFF(LTRIM(RTRIM(CASEMONCOLLABREQUEST_ID)) = '\N',NULL,CASEMONCOLLABREQUEST_ID) AS o_casemoncollabrequest_id
    FROM source_ff_csone_hb_cmc_req_mbrship
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
        previous_role
    FROM transformed_expr
)

SELECT * FROM final