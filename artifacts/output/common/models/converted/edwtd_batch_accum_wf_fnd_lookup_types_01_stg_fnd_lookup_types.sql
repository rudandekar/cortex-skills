{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_fnd_lookup_types', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_FND_LOOKUP_TYPES',
        'target_table': 'STG_FND_LOOKUP_TYPES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.664068+00:00'
    }
) }}

WITH 

source_g2c_fnd_lookup_types AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        application_id,
        lookup_type,
        customization_level,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id,
        view_application_id,
        assign_leaf_only
    FROM {{ source('raw', 'g2c_fnd_lookup_types') }}
),

source_stg_fnd_lookup_types AS (
    SELECT
        application_id,
        lookup_type,
        customization_level,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id,
        view_application_id,
        assign_leaf_only,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_fnd_lookup_types') }}
),

transformed_exp_fnd_lookup_types AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    application_id,
    lookup_type,
    customization_level,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    security_group_id,
    view_application_id,
    assign_leaf_only
    FROM source_stg_fnd_lookup_types
),

final AS (
    SELECT
        application_id,
        lookup_type,
        customization_level,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        security_group_id,
        view_application_id,
        assign_leaf_only,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_fnd_lookup_types
)

SELECT * FROM final