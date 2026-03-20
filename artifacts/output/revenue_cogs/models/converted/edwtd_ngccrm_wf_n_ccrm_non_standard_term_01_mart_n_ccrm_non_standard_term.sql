{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_non_standard_term', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_NON_STANDARD_TERM',
        'target_table': 'N_CCRM_NON_STANDARD_TERM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.494328+00:00'
    }
) }}

WITH 

source_w_ccrm_non_standard_term AS (
    SELECT
        bk_ccrm_non_std_term_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_non_standard_term') }}
),

final AS (
    SELECT
        bk_ccrm_non_std_term_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccrm_non_standard_term
)

SELECT * FROM final