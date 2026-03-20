{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_user_role_map', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_USER_ROLE_MAP',
        'target_table': 'W_IAM_USER_ROLE_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.730331+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_usr_role_tool AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        assigned_app_delim_list,
        user_role_expt_flag,
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
    FROM {{ source('raw', 'st_iam_edwtd_usr_role_tool') }}
),

final AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        standard_or_exception_flg,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        proxy_role,
        ru_proxy_iam_user_key,
        ru_proxy_type_cd,
        action_code,
        dml_type
    FROM source_st_iam_edwtd_usr_role_tool
)

SELECT * FROM final