{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_product_family_link', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_PRODUCT_FAMILY_LINK',
        'target_table': 'EX_IAM_USR_ROLE_PRD_HIER_PF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.755124+00:00'
    }
) }}

WITH 

source_sm_iam_application AS (
    SELECT
        iam_application_key,
        sk_application_id_int,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_iam_application') }}
),

source_n_iam_user AS (
    SELECT
        iam_user_key,
        bk_cpr_universal_id_int,
        internal_role,
        cco_id,
        external_role,
        source_deleted_flg,
        ru_external_user_email_addr,
        ru_cisco_worker_party_key,
        dd_cec_id,
        dd_bk_worker_type_cd,
        sk_iam_user_id_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        dummy_user_role,
        ru_external_flg,
        ru_internal_flg,
        ru_iam_dummy_user_name,
        logging_level_cd
    FROM {{ source('raw', 'n_iam_user') }}
),

source_st_iam_edwtd_usr_rol_prd_hr AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        lvl_number,
        erp_segment1,
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
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_iam_edwtd_usr_rol_prd_hr') }}
),

final AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        lvl_number,
        erp_segment1,
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
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime,
        exception_type
    FROM source_st_iam_edwtd_usr_rol_prd_hr
)

SELECT * FROM final