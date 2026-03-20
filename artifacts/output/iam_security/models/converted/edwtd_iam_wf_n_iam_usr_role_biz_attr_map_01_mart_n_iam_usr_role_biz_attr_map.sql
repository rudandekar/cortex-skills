{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_usr_role_biz_attr_map', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_USR_ROLE_BIZ_ATTR_MAP',
        'target_table': 'N_IAM_USR_ROLE_BIZ_ATTR_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.748183+00:00'
    }
) }}

WITH 

source_w_iam_usr_role_biz_attr_map AS (
    SELECT
        bk_biz_data_attribute_name,
        bk_iam_role_name,
        iam_application_key,
        iam_user_key,
        restrict_or_grant_flg,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_usr_role_biz_attr_map') }}
),

final AS (
    SELECT
        bk_biz_data_attribute_name,
        bk_iam_role_name,
        iam_application_key,
        iam_user_key,
        restrict_or_grant_flg,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_iam_usr_role_biz_attr_map
)

SELECT * FROM final