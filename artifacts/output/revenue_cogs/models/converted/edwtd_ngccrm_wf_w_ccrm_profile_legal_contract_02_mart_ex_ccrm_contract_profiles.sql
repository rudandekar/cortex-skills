{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_ccrm_profile_legal_contract', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_CCRM_PROFILE_LEGAL_CONTRACT',
        'target_table': 'EX_CCRM_CONTRACT_PROFILES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.214819+00:00'
    }
) }}

WITH 

source_el_ccrm_cms_contracts AS (
    SELECT
        contract_id,
        contract_number,
        contract_version,
        contract_status,
        effective_date,
        expiration_date,
        last_update_date,
        creation_datetime
    FROM {{ source('raw', 'el_ccrm_cms_contracts') }}
),

source_st_ccrm_contract_profiles AS (
    SELECT
        batch_id,
        contract_id,
        profile_id,
        created_by,
        creation_date,
        create_timestamp,
        action_code
    FROM {{ source('raw', 'st_ccrm_contract_profiles') }}
),

final AS (
    SELECT
        batch_id,
        contract_id,
        profile_id,
        created_by,
        creation_date,
        create_timestamp,
        action_code,
        exception_type
    FROM source_st_ccrm_contract_profiles
)

SELECT * FROM final