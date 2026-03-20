{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg_fnd_application_tl_ar', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_ST_CG_FND_APPLICATION_TL_AR',
        'target_table': 'ST_CG_FND_APPLICATION_TL_AR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.362035+00:00'
    }
) }}

WITH 

source_cg1_fnd_application_tl AS (
    SELECT
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
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'cg1_fnd_application_tl') }}
),

final AS (
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
    FROM source_cg1_fnd_application_tl
)

SELECT * FROM final