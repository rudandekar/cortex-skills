{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_sub_business_entity_link', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_SUB_BUSINESS_ENTITY_LINK',
        'target_table': 'W_IAM_SUB_BUSINESS_ENTITY_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.762609+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_usr_role_be_hier AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        data_struct_type,
        bk_sub_business_entity_name,
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
    FROM {{ source('raw', 'st_iam_edwtd_usr_role_be_hier') }}
),

final AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        iam_application_key,
        bk_sub_business_entity_name,
        bk_business_entity_type_cd,
        standard_or_exception_flg,
        assignment_type_cd,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_iam_edwtd_usr_role_be_hier
)

SELECT * FROM final