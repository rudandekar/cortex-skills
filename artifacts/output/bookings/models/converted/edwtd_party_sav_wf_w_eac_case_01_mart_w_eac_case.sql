{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_eac_case', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_W_EAC_CASE',
        'target_table': 'W_EAC_CASE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.594634+00:00'
    }
) }}

WITH 

source_st_xxfsam_eac_case_v AS (
    SELECT
        case_id,
        case_type,
        module,
        source,
        status,
        auto_approved,
        effective_date,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        approver,
        reason_code,
        reason_description,
        organization_name,
        organization_id
    FROM {{ source('raw', 'st_xxfsam_eac_case_v') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_xxfsam_eac_case_v
)

SELECT * FROM final