{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_user_role_app_comp_map ', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_USER_ROLE_APP_COMP_MAP ',
        'target_table': 'EX_IAM_EDWTD_USR_ROLE_APP_UCM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.777004+00:00'
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
        last_action,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime,
        exception_type
    FROM source_st_iam_edwtd_usr_role_app
)

SELECT * FROM final