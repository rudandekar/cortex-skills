{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_prfl_evdnc_of_arrngmnt', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_PRFL_EVDNC_OF_ARRNGMNT',
        'target_table': 'N_CCRM_PRFL_EVDNC_OF_ARRNGMNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.360729+00:00'
    }
) }}

WITH 

source_w_ccrm_prfl_evdnc_of_arrngmnt AS (
    SELECT
        bk_ccrm_profile_id_int,
        bk_evdnc_of_arrngmnt_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_prfl_evdnc_of_arrngmnt') }}
),

final AS (
    SELECT
        bk_ccrm_profile_id_int,
        bk_evdnc_of_arrngmnt_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccrm_prfl_evdnc_of_arrngmnt
)

SELECT * FROM final