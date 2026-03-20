{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_role', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_ROLE',
        'target_table': 'N_IAM_ROLE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.723435+00:00'
    }
) }}

WITH 

source_w_iam_role AS (
    SELECT
        bk_iam_role_name,
        iam_role_descr,
        role_category_cd,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_role') }}
),

final AS (
    SELECT
        bk_iam_role_name,
        iam_role_descr,
        role_category_cd,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_iam_role
)

SELECT * FROM final