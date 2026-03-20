{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_warranty_cco_profile_contracts', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_ST_WARRANTY_CCO_PROFILE_CONTRACTS',
        'target_table': 'ST_WARRANTY_CCO_PROFILE_CONTRACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.443661+00:00'
    }
) }}

WITH 

source_ff_warranty_cco_profile_contracts AS (
    SELECT
        party_id,
        party_name,
        party_type,
        contract_number,
        contract_creation_date,
        contract_status,
        inactive_date,
        cco_user,
        cco_creation_date,
        status,
        last_update_date,
        attribute19
    FROM {{ source('raw', 'ff_warranty_cco_profile_contracts') }}
),

final AS (
    SELECT
        party_id,
        party_name,
        party_type,
        contract_number,
        contract_creation_date,
        contract_status,
        inactive_date,
        cco_user,
        cco_creation_date,
        status,
        last_update_date,
        attribute19
    FROM source_ff_warranty_cco_profile_contracts
)

SELECT * FROM final