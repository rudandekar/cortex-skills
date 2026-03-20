{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_iam_edwtd_application', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_FF_IAM_EDWTD_APPLICATION',
        'target_table': 'FF_IAM_EDWTD_APPLICATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.751008+00:00'
    }
) }}

WITH 

source_iam_edwtd_application_vw AS (
    SELECT
        application_id,
        application_name,
        application_descr,
        application_status,
        training_required,
        created_by,
        create_date,
        updated_by,
        update_date
    FROM {{ source('raw', 'iam_edwtd_application_vw') }}
),

transformed_exp_ff_iam_edwtd_application AS (
    SELECT
    application_id,
    application_name,
    application_descr,
    application_status,
    training_required,
    created_by,
    create_date,
    updated_by,
    update_date,
    'I' AS action_code,
    'BatchId' AS batch_id,
    CURRENT_DATE() AS create_datetime
    FROM source_iam_edwtd_application_vw
),

final AS (
    SELECT
        application_id,
        application_name,
        application_descr,
        application_status,
        training_required,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM transformed_exp_ff_iam_edwtd_application
)

SELECT * FROM final