{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_user_role_app_map', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_USER_ROLE_APP_MAP',
        'target_table': 'W_IAM_USER_ROLE_APP_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.732122+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_usr_role_app AS (
    SELECT
        iam_user_id,
        role_id,
        role_name,
        tool_id,
        app_res_id,
        app_res_val,
        app_res_path,
        proxy_flag,
        grantor_universal_id,
        grantor_cec_id,
        grantor_cco_id,
        status,
        tr_flag,
        default_app_id,
        default_app_val,
        last_action,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_iam_edwtd_usr_role_app') }}
),

final AS (
    SELECT
        bk_iam_role_name,
        iam_application_key,
        iam_user_key,
        traning_completed_flg,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        dv_user_partner_cnt,
        default_iam_app_comp_key,
        action_code,
        dml_type
    FROM source_st_iam_edwtd_usr_role_app
)

SELECT * FROM final