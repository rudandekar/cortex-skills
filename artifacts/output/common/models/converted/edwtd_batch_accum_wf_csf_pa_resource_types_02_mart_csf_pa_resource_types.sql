{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_pa_resource_types', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_RESOURCE_TYPES',
        'target_table': 'CSF_PA_RESOURCE_TYPES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.984414+00:00'
    }
) }}

WITH 

source_stg_csf_pa_resource_types AS (
    SELECT
        resource_type_id,
        resource_class_code,
        resource_type_code,
        name,
        description,
        table_name,
        access_key,
        sql_text,
        start_date_active,
        end_date_active,
        group_flag,
        last_updated_by,
        last_update_date,
        creation_date,
        created_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_resource_types') }}
),

source_csf_pa_resource_types AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        resource_type_id,
        resource_class_code,
        resource_type_code,
        name,
        description,
        table_name,
        access_key,
        sql_text,
        start_date_active,
        end_date_active,
        group_flag,
        last_updated_by,
        last_update_date,
        creation_date,
        created_by,
        last_update_login,
        zd_edition_name,
        zd_sync
    FROM {{ source('raw', 'csf_pa_resource_types') }}
),

transformed_exp_csf_pa_resource_types AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    resource_type_id,
    resource_class_code,
    resource_type_code,
    name,
    description,
    table_name,
    access_key,
    sql_text,
    start_date_active,
    end_date_active,
    group_flag,
    last_updated_by,
    last_update_date,
    creation_date,
    created_by,
    last_update_login,
    zd_edition_name,
    zd_sync
    FROM source_csf_pa_resource_types
),

final AS (
    SELECT
        resource_type_id,
        resource_class_code,
        resource_type_code,
        name,
        description,
        table_name,
        access_key,
        sql_text,
        start_date_active,
        end_date_active,
        group_flag,
        last_updated_by,
        last_update_date,
        creation_date,
        created_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_resource_types
)

SELECT * FROM final