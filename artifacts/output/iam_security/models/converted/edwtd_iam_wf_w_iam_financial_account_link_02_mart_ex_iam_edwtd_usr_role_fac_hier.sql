{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_financial_account_link', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_FINANCIAL_ACCOUNT_LINK',
        'target_table': 'EX_IAM_EDWTD_USR_ROLE_FAC_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.743467+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_usr_role_fac_hier AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        node_key,
        erp_segment1,
        lvl_number,
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
    FROM {{ source('raw', 'st_iam_edwtd_usr_role_fac_hier') }}
),

source_w_iam_financial_account_link AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        iam_application_key,
        bk_financial_account_cd,
        assignment_type,
        standard_or_exception_flg,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_financial_account_link') }}
),

final AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        node_key,
        erp_segment1,
        lvl_number,
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
    FROM source_w_iam_financial_account_link
)

SELECT * FROM final