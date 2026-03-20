{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_fnd_application_ar', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_EL_FND_APPLICATION_AR',
        'target_table': 'EL_FND_APPLICATION_AR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.631424+00:00'
    }
) }}

WITH 

source_st_fnd_application_ar AS (
    SELECT
        batch_id,
        application_id,
        lang,
        application_name,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        description,
        source_lang,
        source_commit_time,
        global_name,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_fnd_application_ar') }}
),

final AS (
    SELECT
        application_id,
        global_name,
        lang,
        application_name,
        description,
        edw_create_user,
        edw_create_dtm,
        edw_update_user,
        edw_update_dtm
    FROM source_st_fnd_application_ar
)

SELECT * FROM final