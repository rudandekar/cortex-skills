{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_application_attribute', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_APPLICATION_ATTRIBUTE',
        'target_table': 'W_IAM_APPLICATION_ATTRIBUTE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.728170+00:00'
    }
) }}

WITH 

source_st_iam_edwtd_role_tool AS (
    SELECT
        role_id,
        role_name,
        tool_id,
        role_category,
        description,
        type_name_1,
        type_val_name_1,
        type_name_2,
        type_val_name_2,
        type_name_3,
        type_val_name_3,
        type_name_4,
        type_val_name_4,
        type_name_5,
        type_val_name_5,
        status,
        last_action,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'st_iam_edwtd_role_tool') }}
),

final AS (
    SELECT
        iam_application_key,
        bk_application_attribute_name,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM source_st_iam_edwtd_role_tool
)

SELECT * FROM final