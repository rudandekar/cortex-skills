{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_business_data_attribute', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_BUSINESS_DATA_ATTRIBUTE',
        'target_table': 'N_IAM_BUSINESS_DATA_ATTRIBUTE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.773303+00:00'
    }
) }}

WITH 

source_w_iam_business_data_attribute AS (
    SELECT
        bk_biz_data_attribute_name,
        biz_data_attribute_descr,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_iam_business_data_attribute') }}
),

final AS (
    SELECT
        bk_biz_data_attribute_name,
        biz_data_attribute_descr,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_w_iam_business_data_attribute
)

SELECT * FROM final