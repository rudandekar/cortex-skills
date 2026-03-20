{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_cms_contracts', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_CMS_CONTRACTS',
        'target_table': 'FF_CCRM_CMS_CONTRACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.453682+00:00'
    }
) }}

WITH 

source_ccrm_cms_contracts AS (
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
    FROM {{ source('raw', 'ccrm_cms_contracts') }}
),

transformed_exp_ccrm_cms_contracts AS (
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
    last_update_date,
    'BatchId' AS batchid_out,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_ccrm_cms_contracts
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
        create_datetime,
        action_code
    FROM transformed_exp_ccrm_cms_contracts
)

SELECT * FROM final