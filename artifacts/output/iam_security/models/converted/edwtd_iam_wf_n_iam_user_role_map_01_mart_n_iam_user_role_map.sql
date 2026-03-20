{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_user_role_map', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_USER_ROLE_MAP',
        'target_table': 'N_IAM_USER_ROLE_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.743082+00:00'
    }
) }}

WITH 

source_w_iam_user_role_map AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        standard_or_exception_flg,
        proxy_role,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        ru_proxy_iam_user_key,
        ru_proxy_type_cd,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_user_role_map') }}
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
        ru_proxy_type_cd
    FROM source_w_iam_user_role_map
)

SELECT * FROM final