{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_user_role_app_comp_map', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_USER_ROLE_APP_COMP_MAP',
        'target_table': 'N_IAM_USER_ROLE_APP_COMP_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.759199+00:00'
    }
) }}

WITH 

source_w_iam_user_role_app_comp_map AS (
    SELECT
        bk_iam_role_name,
        iam_application_key,
        iam_application_component_key,
        iam_user_key,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_user_role_app_comp_map') }}
),

final AS (
    SELECT
        bk_iam_role_name,
        iam_application_key,
        iam_application_component_key,
        iam_user_key,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_iam_user_role_app_comp_map
)

SELECT * FROM final