{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_business_function_link', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_BUSINESS_FUNCTION_LINK',
        'target_table': 'N_IAM_BUSINESS_FUNCTION_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.718832+00:00'
    }
) }}

WITH 

source_w_iam_business_function_link AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        iam_application_key,
        bk_business_function_name,
        standard_or_exception_flg,
        assignment_type_cd,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_business_function_link') }}
),

final AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        iam_application_key,
        bk_business_function_name,
        standard_or_exception_flg,
        assignment_type_cd,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_iam_business_function_link
)

SELECT * FROM final