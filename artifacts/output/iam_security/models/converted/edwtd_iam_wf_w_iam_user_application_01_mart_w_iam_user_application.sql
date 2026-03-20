{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_user_application', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_USER_APPLICATION',
        'target_table': 'W_IAM_USER_APPLICATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.765516+00:00'
    }
) }}

WITH 

source_w_iam_user_application AS (
    SELECT
        iam_user_key,
        iam_application_key,
        edwtd_login_type_cd,
        purge_usage_flg,
        admin_access_flg,
        revalidation_cd,
        proxy_access_flg,
        log_level_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_user_application') }}
),

final AS (
    SELECT
        iam_user_key,
        iam_application_key,
        edwtd_login_type_cd,
        purge_usage_flg,
        admin_access_flg,
        revalidation_cd,
        proxy_access_flg,
        bi_representative_party_key,
        log_level_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM source_w_iam_user_application
)

SELECT * FROM final