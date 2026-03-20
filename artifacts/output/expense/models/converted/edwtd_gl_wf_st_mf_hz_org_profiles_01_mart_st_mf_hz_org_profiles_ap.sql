{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_mf_hz_org_profiles', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_MF_HZ_ORG_PROFILES',
        'target_table': 'ST_MF_HZ_ORG_PROFILES_AP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.167732+00:00'
    }
) }}

WITH 

source_ff_mf_hz_org_profiles_ap AS (
    SELECT
        batch_id,
        organization_profile_id,
        party_id,
        organization_name,
        bank_or_branch_number,
        last_update_date,
        ges_update_date,
        global_name,
        creation_datetime,
        action_code
    FROM {{ source('raw', 'ff_mf_hz_org_profiles_ap') }}
),

final AS (
    SELECT
        batch_id,
        organization_profile_id,
        party_id,
        organization_name,
        bank_or_branch_number,
        last_update_date,
        ges_update_date,
        global_name,
        creation_datetime,
        action_code
    FROM source_ff_mf_hz_org_profiles_ap
)

SELECT * FROM final