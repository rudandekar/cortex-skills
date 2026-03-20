{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_iam_edwtd_app_component', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_FF_IAM_EDWTD_APP_COMPONENT',
        'target_table': 'FF_IAM_EDWTD_APP_COMPONENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.742381+00:00'
    }
) }}

WITH 

source_iam_edwtd_app_component_vw AS (
    SELECT
        application_id,
        app_component_id,
        par_app_componet_id,
        app_component_name,
        app_component_descr,
        app_component_type,
        leaf_branch,
        app_component_status,
        created_by,
        create_date,
        updated_by,
        update_date
    FROM {{ source('raw', 'iam_edwtd_app_component_vw') }}
),

transformed_exp_ff_iam_edwtd_app_component AS (
    SELECT
    application_id,
    app_component_id,
    par_app_componet_id,
    app_component_name,
    app_component_descr,
    app_component_type,
    leaf_branch,
    app_component_status,
    created_by,
    create_date,
    updated_by,
    update_date,
    'I' AS action_code,
    'BatchId' AS batch_id,
    CURRENT_DATE() AS create_datetime
    FROM source_iam_edwtd_app_component_vw
),

final AS (
    SELECT
        application_id,
        app_component_id,
        par_app_componet_id,
        app_component_name,
        app_component_descr,
        app_component_type,
        leaf_branch,
        app_component_status,
        created_by,
        create_date,
        updated_by,
        update_date,
        action_code,
        batch_id,
        create_datetime
    FROM transformed_exp_ff_iam_edwtd_app_component
)

SELECT * FROM final