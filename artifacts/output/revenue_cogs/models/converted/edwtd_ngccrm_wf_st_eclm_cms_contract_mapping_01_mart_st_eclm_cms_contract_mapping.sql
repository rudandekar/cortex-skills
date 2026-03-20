{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_eclm_cms_contract_mapping', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_ECLM_CMS_CONTRACT_MAPPING',
        'target_table': 'ST_ECLM_CMS_CONTRACT_MAPPING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.805736+00:00'
    }
) }}

WITH 

source_ff_eclm_cms_contract_mapping AS (
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
    FROM {{ source('raw', 'ff_eclm_cms_contract_mapping') }}
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
    FROM source_ff_eclm_cms_contract_mapping
)

SELECT * FROM final