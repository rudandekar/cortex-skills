{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_application', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_APPLICATION',
        'target_table': 'N_IAM_APPLICATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.720463+00:00'
    }
) }}

WITH 

source_w_iam_application AS (
    SELECT
        iam_application_key,
        training_required_flg,
        application_name,
        sk_application_id_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_application') }}
),

final AS (
    SELECT
        iam_application_key,
        training_required_flg,
        application_name,
        sk_application_id_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_iam_application
)

SELECT * FROM final