{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_eac_org', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_N_EAC_ORG',
        'target_table': 'N_EAC_ORG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.546432+00:00'
    }
) }}

WITH 

source_w_eac_org AS (
    SELECT
        bk_eac_org_id,
        eac_org_name,
        org_start_dt,
        org_end_dt,
        src_created_dtm,
        dv_src_created_dt,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        src_lst_uptd_dtm,
        dv_src_last_uptd_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_eac_org') }}
),

final AS (
    SELECT
        bk_eac_org_id,
        eac_org_name,
        org_start_dt,
        org_end_dt,
        src_created_dtm,
        dv_src_created_dt,
        crtd_by_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        src_lst_uptd_dtm,
        dv_src_last_uptd_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_eac_org
)

SELECT * FROM final