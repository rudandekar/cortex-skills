{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_warranty_cco_profile_contracts', 'batch', 'edwtd_migration'],
    meta={
        'source_workflow': 'wf_m_FF_WARRANTY_CCO_PROFILE_CONTRACTS',
        'target_table': 'FF_WARRANTY_CCO_PROFILE_CONTRACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.750991+00:00'
    }
) }}

WITH 

source_xxcts_cco_profile_contracts AS (
    SELECT
        party_id,
        contract_id,
        contract_number,
        contract_status,
        orig_system_reference,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        inactive_date,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        cco_profile_contract_id,
        bid_association_status,
        direct_cont_asso_status,
        party_name,
        party_type,
        attribute17,
        cco_creation_date,
        status,
        cco_last_update_date,
        attribute19
    FROM {{ source('raw', 'xxcts_cco_profile_contracts') }}
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
    FROM source_xxcts_cco_profile_contracts
)

SELECT * FROM final