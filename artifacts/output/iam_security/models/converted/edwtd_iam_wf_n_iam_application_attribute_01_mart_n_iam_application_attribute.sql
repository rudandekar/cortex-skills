{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_application_attribute', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_APPLICATION_ATTRIBUTE',
        'target_table': 'N_IAM_APPLICATION_ATTRIBUTE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.762330+00:00'
    }
) }}

WITH 

source_w_iam_application_attribute AS (
    SELECT
        iam_application_key,
        bk_application_attribute_name,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_application_attribute') }}
),

final AS (
    SELECT
        iam_application_key,
        bk_application_attribute_name,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_iam_application_attribute
)

SELECT * FROM final