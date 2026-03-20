{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_role_app_component_map', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_ROLE_APP_COMPONENT_MAP',
        'target_table': 'W_IAM_ROLE_APP_COMPONENT_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.753287+00:00'
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
        bk_iam_role_name,
        iam_application_key,
        iam_application_component_key,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM source_st_iam_edwtd_usr_role_app
)

SELECT * FROM final