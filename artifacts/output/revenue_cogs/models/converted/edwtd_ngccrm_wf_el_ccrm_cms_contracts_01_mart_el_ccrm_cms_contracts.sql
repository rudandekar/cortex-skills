{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccrm_cms_contracts', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_EL_CCRM_CMS_CONTRACTS',
        'target_table': 'EL_CCRM_CMS_CONTRACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.003744+00:00'
    }
) }}

WITH 

source_st_ccrm_cms_contracts AS (
    SELECT
        batch_id,
        contract_id,
        contract_number,
        contract_version,
        contract_status,
        effective_date,
        expiration_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        current_datetime,
        action_code
    FROM {{ source('raw', 'st_ccrm_cms_contracts') }}
),

final AS (
    SELECT
        contract_id,
        contract_number,
        contract_version,
        contract_status,
        effective_date,
        expiration_date,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date
    FROM source_st_ccrm_cms_contracts
)

SELECT * FROM final