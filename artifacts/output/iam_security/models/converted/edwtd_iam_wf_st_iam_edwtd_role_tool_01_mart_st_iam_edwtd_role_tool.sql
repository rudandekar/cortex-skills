{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_iam_edwtd_role_tool', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_ST_IAM_EDWTD_ROLE_TOOL',
        'target_table': 'ST_IAM_EDWTD_ROLE_TOOL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.728481+00:00'
    }
) }}

WITH 

source_ff_iam_edwtd_role_tool AS (
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
    FROM {{ source('raw', 'ff_iam_edwtd_role_tool') }}
),

final AS (
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
    FROM source_ff_iam_edwtd_role_tool
)

SELECT * FROM final