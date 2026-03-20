{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_contract_profiles', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_CONTRACT_PROFILES',
        'target_table': 'FF_CCRM_CONTRACT_PROFILES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.254068+00:00'
    }
) }}

WITH 

source_ccrm_contract_profiles AS (
    SELECT
        contract_id,
        profile_id,
        created_by,
        creation_date
    FROM {{ source('raw', 'ccrm_contract_profiles') }}
),

transformed_exp_ccrm_contract_profiles AS (
    SELECT
    contract_id,
    profile_id,
    created_by,
    creation_date,
    'BATCH_ID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_timestamp,
    'I' AS action_code
    FROM source_ccrm_contract_profiles
),

final AS (
    SELECT
        batch_id,
        contract_id,
        profile_id,
        created_by,
        creation_date,
        create_timestamp,
        action_code
    FROM transformed_exp_ccrm_contract_profiles
)

SELECT * FROM final