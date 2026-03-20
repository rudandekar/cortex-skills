{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_ptnrs_hier_lnk', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_PTNRS_HIER_LNK',
        'target_table': 'W_IAM_PTNRS_HIER_LNK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.774886+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_usr_role_ptnrsite AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        lvl_number,
        site_target_id,
        assignment_type,
        data_res_expt_flag,
        excl_restr_flag,
        restriction_flag,
        proxy_flag,
        grantor_universal_id,
        grantor_cec_id,
        grantor_cco_id,
        status,
        tr_flag,
        last_action,
        erp_segment1,
        is_user_sel_par,
        pdb_site_id,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_iam_edwtd_usr_role_ptnrsite') }}
),

final AS (
    SELECT
        partner_site_party_key,
        bk_iam_role_name,
        iam_application_key,
        iam_user_key,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        dd_bk_prtnr_site_party_id_int,
        iam_level_num_int,
        source_deleted_flg,
        dd_cec_id,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM source_st_iam_edwtd_usr_role_ptnrsite
)

SELECT * FROM final