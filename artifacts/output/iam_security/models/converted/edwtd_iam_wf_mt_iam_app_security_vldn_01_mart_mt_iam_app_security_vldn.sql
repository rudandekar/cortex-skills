{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_iam_app_security_vldn', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_MT_IAM_APP_SECURITY_VLDN',
        'target_table': 'MT_IAM_APP_SECURITY_VLDN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.714457+00:00'
    }
) }}

WITH 

source_mt_iam_app_security_vldn AS (
    SELECT
        cec_id,
        dv_application_name,
        dv_category_cd,
        dv_data_type_cd,
        dv_data_value_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_iam_app_security_vldn') }}
),

final AS (
    SELECT
        cec_id,
        dv_application_name,
        dv_category_cd,
        dv_data_type_cd,
        dv_data_value_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_iam_app_security_vldn
)

SELECT * FROM final