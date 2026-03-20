{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_eac_case_cmnt', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_N_EAC_CASE_CMNT',
        'target_table': 'N_EAC_CASE_CMNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.759476+00:00'
    }
) }}

WITH 

source_w_eac_case_cmnt AS (
    SELECT
        bk_eac_case_comment_id,
        crtd_by_csco_wrkr_prty_key,
        bk_eac_case_id,
        comment_txt,
        src_last_uptd_dtm,
        dv_src_last_uptd_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_eac_case_cmnt') }}
),

final AS (
    SELECT
        bk_eac_case_comment_id,
        crtd_by_csco_wrkr_prty_key,
        bk_eac_case_id,
        comment_txt,
        src_last_uptd_dtm,
        dv_src_last_uptd_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_eac_case_cmnt
)

SELECT * FROM final