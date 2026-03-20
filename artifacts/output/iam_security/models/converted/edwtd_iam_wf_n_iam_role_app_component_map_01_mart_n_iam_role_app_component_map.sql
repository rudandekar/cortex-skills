{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_role_app_component_map', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_ROLE_APP_COMPONENT_MAP',
        'target_table': 'N_IAM_ROLE_APP_COMPONENT_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.743754+00:00'
    }
) }}

WITH 

source_w_iam_role_app_component_map AS (
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
    FROM {{ source('raw', 'w_iam_role_app_component_map') }}
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
        edw_update_dtm
    FROM source_w_iam_role_app_component_map
)

SELECT * FROM final