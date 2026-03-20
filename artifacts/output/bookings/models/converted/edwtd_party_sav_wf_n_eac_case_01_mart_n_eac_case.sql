{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_eac_case', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_N_EAC_CASE',
        'target_table': 'N_EAC_CASE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.380745+00:00'
    }
) }}

WITH 

source_w_eac_case AS (
    SELECT
        bk_eac_case_id,
        bk_eac_org_id,
        case_action_type_name,
        case_module_cd,
        upload_source_name,
        case_status_name,
        auto_approved_flg,
        effective_dt,
        crtd_by_csco_wrkr_prty_key,
        approver_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        case_reason_cd,
        src_created_dtm,
        case_reason_descr,
        dv_src_created_dt,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_eac_case') }}
),

final AS (
    SELECT
        bk_eac_case_id,
        bk_eac_org_id,
        case_action_type_name,
        case_module_cd,
        upload_source_name,
        case_status_name,
        auto_approved_flg,
        effective_dt,
        crtd_by_csco_wrkr_prty_key,
        approver_csco_wrkr_prty_key,
        lst_uptd_by_csco_wrkr_prty_key,
        case_reason_cd,
        src_created_dtm,
        case_reason_descr,
        dv_src_created_dt,
        src_last_updated_dtm,
        dv_src_last_updated_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_eac_case
)

SELECT * FROM final