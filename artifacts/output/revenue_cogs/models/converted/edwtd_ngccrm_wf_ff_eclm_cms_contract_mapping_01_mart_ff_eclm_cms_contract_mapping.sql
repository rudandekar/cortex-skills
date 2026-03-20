{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_eclm_cms_contract_mapping', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_ECLM_CMS_CONTRACT_MAPPING',
        'target_table': 'FF_ECLM_CMS_CONTRACT_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.353141+00:00'
    }
) }}

WITH 

source_eclm_cms_contract_mapping AS (
    SELECT
        eclm_contract_number,
        cms_contract_number,
        cms_version,
        related_contract_number,
        relationship,
        contract_negotiator,
        eclm_contract_status,
        effective_start_date,
        expiration_date,
        updated_date
    FROM {{ source('raw', 'eclm_cms_contract_mapping') }}
),

transformed_exp_eclm_cms_contract_mapping AS (
    SELECT
    eclm_contract_number,
    cms_contract_number,
    cms_version,
    related_contract_number,
    relationship,
    contract_negotiator,
    eclm_contract_status,
    effective_start_date,
    expiration_date,
    updated_date,
    'BATCH_ID' AS batch_id,
    CURRENT_TIMESTAMP() AS create_timestamp,
    'I' AS action_code
    FROM source_eclm_cms_contract_mapping
),

final AS (
    SELECT
        batch_id,
        eclm_contract_number,
        cms_contract_number,
        cms_version,
        related_contract_number,
        relationship,
        contract_negotiator,
        eclm_contract_status,
        effective_start_date,
        expiration_date,
        updated_date,
        create_timestamp,
        action_code
    FROM transformed_exp_eclm_cms_contract_mapping
)

SELECT * FROM final