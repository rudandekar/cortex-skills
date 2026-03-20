{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_legal_contract', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_LEGAL_CONTRACT',
        'target_table': 'N_LEGAL_CONTRACT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.247757+00:00'
    }
) }}

WITH 

source_w_legal_contract AS (
    SELECT
        bk_contract_number_int,
        parent_contract_num_int,
        contract_trx_type_cd,
        contract_effective_dt,
        contract_expiration_dt,
        contract_status_cd,
        cms_reference_role,
        ru_cms_contract_num_int,
        ru_cms_contract_ver_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_legal_contract') }}
),

final AS (
    SELECT
        bk_contract_number_int,
        contract_effective_dt,
        contract_expiration_dt,
        contract_status_cd,
        cms_reference_role,
        ru_cms_contract_num_int,
        ru_cms_contract_ver_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_legal_contract
)

SELECT * FROM final