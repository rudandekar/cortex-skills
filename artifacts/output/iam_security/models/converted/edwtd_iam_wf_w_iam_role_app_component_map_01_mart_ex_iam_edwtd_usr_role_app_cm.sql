{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_role_app_component_map', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_ROLE_APP_COMPONENT_MAP',
        'target_table': 'EX_IAM_EDWTD_USR_ROLE_APP_CM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.734177+00:00'
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