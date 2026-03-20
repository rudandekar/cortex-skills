{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_profile_non_std_term', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_PROFILE_NON_STD_TERM',
        'target_table': 'N_CCRM_PROFILE_NON_STD_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.240806+00:00'
    }
) }}

WITH 

source_w_ccrm_profile_non_std_term AS (
    SELECT
        bk_ccrm_non_std_term_name,
        bk_ccrm_profile_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_profile_non_std_term') }}
),

final AS (
    SELECT
        bk_ccrm_non_std_term_name,
        bk_ccrm_profile_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccrm_profile_non_std_term
)

SELECT * FROM final