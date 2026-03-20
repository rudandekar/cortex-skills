{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_business_data_attribute', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_BUSINESS_DATA_ATTRIBUTE',
        'target_table': 'W_IAM_BUSINESS_DATA_ATTRIBUTE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.757756+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_biz_data_attrib AS (
    SELECT
        biz_data_attrib_name,
        biz_data_attrib_descr,
        status,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_iam_edwtd_biz_data_attrib') }}
),

final AS (
    SELECT
        bk_biz_data_attribute_name,
        biz_data_attribute_descr,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM source_st_iam_edwtd_biz_data_attrib
)

SELECT * FROM final