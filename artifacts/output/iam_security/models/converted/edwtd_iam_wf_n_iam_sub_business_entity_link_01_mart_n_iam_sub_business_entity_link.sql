{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_sub_business_entity_link', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_SUB_BUSINESS_ENTITY_LINK',
        'target_table': 'N_IAM_SUB_BUSINESS_ENTITY_LINK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.725928+00:00'
    }
) }}

WITH 

source_n_iam_sub_business_entity_link AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        iam_application_key,
        bk_sub_business_entity_name,
        bk_business_entity_type_cd,
        standard_or_exception_flg,
        assignment_type_cd,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_iam_sub_business_entity_link') }}
),

final AS (
    SELECT
        bk_iam_role_name,
        iam_user_key,
        iam_application_key,
        bk_sub_business_entity_name,
        bk_business_entity_type_cd,
        standard_or_exception_flg,
        assignment_type_cd,
        exclusive_or_restrictive_flg,
        source_deleted_flg,
        iam_level_num_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_iam_sub_business_entity_link
)

SELECT * FROM final