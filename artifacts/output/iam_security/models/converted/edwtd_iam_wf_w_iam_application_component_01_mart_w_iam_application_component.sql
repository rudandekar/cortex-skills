{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_iam_application_component', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_W_IAM_APPLICATION_COMPONENT',
        'target_table': 'W_IAM_APPLICATION_COMPONENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.719845+00:00'
    }
) }}

WITH 

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
        bk_application_component_name,
        application_component_descr,
        status_cd,
        application_type_cd,
        leaf_branch_flg,
        iam_application_key,
        sk_app_component_id_int,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm,
        action_code,
        dml_type
    FROM source_st_iam_edwtd_app_component
)

SELECT * FROM final