{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_user_role_app_map', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_USER_ROLE_APP_MAP',
        'target_table': 'N_IAM_USER_ROLE_APP_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.771394+00:00'
    }
) }}

WITH 

source_w_iam_user_role_app_map AS (
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
    FROM {{ source('raw', 'w_iam_user_role_app_map') }}
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
        default_iam_app_comp_key
    FROM source_w_iam_user_role_app_map
)

SELECT * FROM final