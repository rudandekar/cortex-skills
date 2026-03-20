{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_iam_user', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_WI_IAM_USER',
        'target_table': 'WI_IAM_USER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.769283+00:00'
    }
) }}

WITH 

source_sm_iam_user AS (
    SELECT
        iam_user_key,
        sk_iam_user_id_int,
        edw_create_user,
        edw_create_dtm
    FROM {{ source('raw', 'sm_iam_user') }}
),

final AS (
    SELECT
        iam_user_key,
        sk_iam_user_id_int,
        source_deleted_flg,
        edw_create_user,
        edw_create_dtm
    FROM source_sm_iam_user
)

SELECT * FROM final