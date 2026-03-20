{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_iam_application_component', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_SM_IAM_APPLICATION_COMPONENT',
        'target_table': 'SM_IAM_APPLICATION_COMPONENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.722679+00:00'
    }
) }}

WITH 

source_ex_iam_edwtd_app_component AS (
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
        create_datetime,
        exception_type
    FROM {{ source('raw', 'ex_iam_edwtd_app_component') }}
),

source_st_iam_edwtd_app_component AS (
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
    FROM {{ source('raw', 'st_iam_edwtd_app_component') }}
),

final AS (
    SELECT
        iam_application_component_key,
        sk_app_component_id_int,
        edw_create_user,
        edw_create_dtm
    FROM source_st_iam_edwtd_app_component
)

SELECT * FROM final