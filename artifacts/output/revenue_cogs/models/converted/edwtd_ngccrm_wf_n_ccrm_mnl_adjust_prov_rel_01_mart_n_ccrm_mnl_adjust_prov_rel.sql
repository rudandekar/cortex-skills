{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_ccrm_mnl_adjust_prov_rel', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_N_CCRM_MNL_ADJUST_PROV_REL',
        'target_table': 'N_CCRM_MNL_ADJUST_PROV_REL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.146631+00:00'
    }
) }}

WITH 

source_w_ccrm_mnl_adjust_prov_rel AS (
    SELECT
        provision_release_type_cd,
        ccrm_manual_adjustment_key,
        provision_release_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_ccrm_mnl_adjust_prov_rel') }}
),

final AS (
    SELECT
        provision_release_type_cd,
        ccrm_manual_adjustment_key,
        provision_release_usd_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_ccrm_mnl_adjust_prov_rel
)

SELECT * FROM final