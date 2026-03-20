{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_iam_edwtd_usr_role_app', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_FF_IAM_EDWTD_USR_ROLE_APP',
        'target_table': 'FF_IAM_EDWTD_USR_ROLE_APP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.789555+00:00'
    }
) }}

WITH 

source_iam_edwtd_usr_role_app_vw AS (
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
        update_date
    FROM {{ source('raw', 'iam_edwtd_usr_role_app_vw') }}
),

transformed_exp_ff_iam_edwtd_usr_role_app AS (
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
    'I' AS action_code,
    'BatchId' AS batch_id,
    CURRENT_DATE() AS create_datetime
    FROM source_iam_edwtd_usr_role_app_vw
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
    FROM transformed_exp_ff_iam_edwtd_usr_role_app
)

SELECT * FROM final