{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_eac_group_member_sav', 'batch', 'edwtd_party_sav'],
    meta={
        'source_workflow': 'wf_m_N_EAC_GROUP_MEMBER_SAV',
        'target_table': 'N_EAC_GROUP_MEMBER_SAV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.463009+00:00'
    }
) }}

WITH 

source_w_eac_group_member_sav AS (
    SELECT
        bk_eac_group_member_id,
        sls_acct_grp_prty_key,
        bk_eac_group_id,
        bk_effective_dt,
        expiration_dt,
        src_last_rptd_uptd_dtm,
        dv_src_last_uptd_dt,
        crtd_by_csco_wrkr_prty_key,
        bk_eac_org_id,
        current_status_name,
        src_created_dtm,
        dv_src_created_dt,
        src_lst_upd_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_eac_group_member_sav') }}
),

final AS (
    SELECT
        bk_eac_group_member_id,
        sls_acct_grp_prty_key,
        bk_eac_group_id,
        bk_effective_dt,
        expiration_dt,
        src_last_rptd_uptd_dtm,
        dv_src_last_uptd_dt,
        crtd_by_csco_wrkr_prty_key,
        bk_eac_org_id,
        current_status_name,
        src_created_dtm,
        dv_src_created_dt,
        src_lst_upd_csco_wrkr_prty_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_eac_group_member_sav
)

SELECT * FROM final