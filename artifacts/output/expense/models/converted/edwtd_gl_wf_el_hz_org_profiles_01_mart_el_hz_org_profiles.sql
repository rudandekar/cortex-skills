{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_hz_org_profiles ', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_HZ_ORG_PROFILES ',
        'target_table': 'EL_HZ_ORG_PROFILES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.669695+00:00'
    }
) }}

WITH 

source_st_mf_hz_org_profiles_ap AS (
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
    FROM {{ source('raw', 'st_mf_hz_org_profiles_ap') }}
),

final AS (
    SELECT
        organization_profile_id,
        party_id,
        organization_name,
        bank_or_branch_number,
        last_update_date,
        ges_update_date,
        global_name,
        creation_datetime
    FROM source_st_mf_hz_org_profiles_ap
)

SELECT * FROM final