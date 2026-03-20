{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_iam_application_component', 'batch', 'edwtd_iam'],
    meta={
        'source_workflow': 'wf_m_N_IAM_APPLICATION_COMPONENT',
        'target_table': 'N_IAM_APPLICATION_COMPONENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:25:46.774177+00:00'
    }
) }}

WITH 

source_w_iam_application_component AS (
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
    FROM {{ source('raw', 'w_iam_application_component') }}
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
        edw_update_dtm
    FROM source_w_iam_application_component
)

SELECT * FROM final