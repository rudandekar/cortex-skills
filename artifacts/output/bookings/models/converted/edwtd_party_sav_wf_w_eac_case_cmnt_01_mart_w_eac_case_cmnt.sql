{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_eac_case_cmnt', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_W_EAC_CASE_CMNT',
        'target_table': 'W_EAC_CASE_CMNT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.262772+00:00'
    }
) }}

WITH 

source_st_xxfsam_eac_case_comments_v AS (
    SELECT
        comment_id,
        case_id,
        text,
        comment_last_update_date,
        commented_by,
        organization_name,
        organization_id
    FROM {{ source('raw', 'st_xxfsam_eac_case_comments_v') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_xxfsam_eac_case_comments_v
)

SELECT * FROM final