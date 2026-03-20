{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_legal_contract', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_W_LEGAL_CONTRACT',
        'target_table': 'W_LEGAL_CONTRACT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.993771+00:00'
    }
) }}

WITH 

source_st_eclm_cms_contract_mapping AS (
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
    FROM {{ source('raw', 'st_eclm_cms_contract_mapping') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_eclm_cms_contract_mapping
)

SELECT * FROM final