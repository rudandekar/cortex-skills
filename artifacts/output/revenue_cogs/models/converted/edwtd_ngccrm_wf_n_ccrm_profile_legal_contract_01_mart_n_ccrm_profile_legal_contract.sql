{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_profile_legal_contract', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_PROFILE_LEGAL_CONTRACT',
        'target_table': 'N_CCRM_PROFILE_LEGAL_CONTRACT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.892166+00:00'
    }
) }}

WITH 

source_w_ccrm_profile_legal_contract AS (
    SELECT
        bk_ccrm_profile_id_int,
        bk_contract_number_int,
        ccrm_cntrct_basis_for_prfl_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_profile_legal_contract') }}
),

final AS (
    SELECT
        bk_ccrm_profile_id_int,
        bk_contract_number_int,
        ccrm_cntrct_basis_for_prfl_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccrm_profile_legal_contract
)

SELECT * FROM final