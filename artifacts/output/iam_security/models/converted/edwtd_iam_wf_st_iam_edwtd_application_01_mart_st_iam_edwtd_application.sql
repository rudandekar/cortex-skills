{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_iam_edwtd_application', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_ST_IAM_EDWTD_APPLICATION',
        'target_table': 'ST_IAM_EDWTD_APPLICATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.788059+00:00'
    }
) }}

WITH 

source_ff_iam_edwtd_application AS (
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
    FROM {{ source('raw', 'ff_iam_edwtd_application') }}
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
    FROM source_ff_iam_edwtd_application
)

SELECT * FROM final