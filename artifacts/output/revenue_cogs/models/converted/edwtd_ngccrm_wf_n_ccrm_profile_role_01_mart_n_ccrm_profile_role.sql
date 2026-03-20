{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_profile_role', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_PROFILE_ROLE',
        'target_table': 'N_CCRM_PROFILE_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.571854+00:00'
    }
) }}

WITH 

source_w_ccrm_profile_role AS (
    SELECT
        bk_ccrm_profile_role_cd,
        ccrm_profile_role_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_profile_role') }}
),

final AS (
    SELECT
        bk_ccrm_profile_role_cd,
        ccrm_profile_role_descr,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccrm_profile_role
)

SELECT * FROM final