{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_iam_edwtd_usr_app_attr', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_ST_IAM_EDWTD_USR_APP_ATTR',
        'target_table': 'ST_IAM_EDWTD_USR_APP_ATTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.718054+00:00'
    }
) }}

WITH 

source_ff_iam_edwtd_usr_app_attr AS (
    SELECT
        application_id,
        app_name,
        app_description,
        user_id,
        attr_name,
        attr_description,
        attr_value,
        attr_value_description,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM {{ source('raw', 'ff_iam_edwtd_usr_app_attr') }}
),

final AS (
    SELECT
        application_id,
        app_name,
        app_description,
        user_id,
        attr_name,
        attr_description,
        attr_value,
        attr_value_description,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM source_ff_iam_edwtd_usr_app_attr
)

SELECT * FROM final