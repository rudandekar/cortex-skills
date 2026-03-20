{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_fnd_application', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_FND_APPLICATION',
        'target_table': 'STG_CG1_FND_APPLICATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.729000+00:00'
    }
) }}

WITH 

source_stg_cg1_fnd_application AS (
    SELECT
        application_id,
        application_short_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        basepath,
        product_code,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_fnd_application') }}
),

source_cg1_fnd_application AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        application_id,
        application_short_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        basepath,
        product_code
    FROM {{ source('raw', 'cg1_fnd_application') }}
),

transformed_exp_cg1_fnd_application AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    application_id,
    application_short_name,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    basepath,
    product_code
    FROM source_cg1_fnd_application
),

final AS (
    SELECT
        application_id,
        application_short_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        basepath,
        product_code,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_fnd_application
)

SELECT * FROM final