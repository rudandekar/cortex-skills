{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_legal_contract_link', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_LEGAL_CONTRACT_LINK',
        'target_table': 'N_LEGAL_CONTRACT_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.584438+00:00'
    }
) }}

WITH 

source_w_legal_contract_link AS (
    SELECT
        bk_from_contract_num_int,
        bk_to_contract_num_int,
        contract_transaction_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_legal_contract_link') }}
),

final AS (
    SELECT
        bk_from_contract_num_int,
        bk_to_contract_num_int,
        contract_transaction_type_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_legal_contract_link
)

SELECT * FROM final