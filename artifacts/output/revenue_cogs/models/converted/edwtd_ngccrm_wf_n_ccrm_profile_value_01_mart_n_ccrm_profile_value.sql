{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_profile_value', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_PROFILE_VALUE',
        'target_table': 'N_CCRM_PROFILE_VALUE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.294319+00:00'
    }
) }}

WITH 

source_w_ccrm_profile_value AS (
    SELECT
        bk_ccrm_profile_id_int,
        bk_ccrm_element_type_cd,
        ccrm_profile_value_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_profile_value') }}
),

final AS (
    SELECT
        bk_ccrm_profile_id_int,
        bk_ccrm_element_type_cd,
        ccrm_profile_value_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccrm_profile_value
)

SELECT * FROM final