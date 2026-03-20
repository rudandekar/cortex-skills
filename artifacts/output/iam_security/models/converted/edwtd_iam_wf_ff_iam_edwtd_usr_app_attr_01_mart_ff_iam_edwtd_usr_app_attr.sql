{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_iam_edwtd_usr_app_attr', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_FF_IAM_EDWTD_USR_APP_ATTR',
        'target_table': 'FF_IAM_EDWTD_USR_APP_ATTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.744709+00:00'
    }
) }}

WITH 

source_iam_edwtd_usr_app_attr_vw AS (
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
        update_date
    FROM {{ source('raw', 'iam_edwtd_usr_app_attr_vw') }}
),

transformed_exp_iam_edwtd_usr_role_acc_hier AS (
    SELECT
    iam_user_id,
    'I' AS action_code,
    ''BatchId'' AS batch_id,
    CURRENT_DATE() AS create_datetime
    FROM source_iam_edwtd_usr_app_attr_vw
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
    FROM transformed_exp_iam_edwtd_usr_role_acc_hier
)

SELECT * FROM final