{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_cms_contracts', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_CMS_CONTRACTS',
        'target_table': 'ST_CCRM_CMS_CONTRACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.181747+00:00'
    }
) }}

WITH 

source_ff_ccrm_cms_contracts AS (
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
    FROM {{ source('raw', 'ff_ccrm_cms_contracts') }}
),

final AS (
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
        creation_datetime,
        action_code
    FROM source_ff_ccrm_cms_contracts
)

SELECT * FROM final