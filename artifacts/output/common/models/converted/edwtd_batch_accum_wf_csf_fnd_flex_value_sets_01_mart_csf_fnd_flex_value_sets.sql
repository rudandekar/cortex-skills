{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_fnd_flex_value_sets', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_FND_FLEX_VALUE_SETS',
        'target_table': 'CSF_FND_FLEX_VALUE_SETS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.813108+00:00'
    }
) }}

WITH 

source_stg_csf_fnd_flex_value_sets AS (
    SELECT
        flex_value_set_id,
        flex_value_set_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        validation_type,
        protected_flag,
        security_enabled_flag,
        longlist_flag,
        format_type,
        maximum_size,
        alphanumeric_allowed_flag,
        uppercase_only_flag,
        numeric_mode_enabled_flag,
        description,
        dependant_default_value,
        dependant_default_meaning,
        parent_flex_value_set_id,
        minimum_value,
        maximum_value,
        number_precision,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_fnd_flex_value_sets') }}
),

source_csf_fnd_flex_value_sets AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        flex_value_set_id,
        flex_value_set_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        validation_type,
        protected_flag,
        security_enabled_flag,
        longlist_flag,
        format_type,
        maximum_size,
        alphanumeric_allowed_flag,
        uppercase_only_flag,
        numeric_mode_enabled_flag,
        description,
        dependant_default_value,
        dependant_default_meaning,
        parent_flex_value_set_id,
        minimum_value,
        maximum_value,
        number_precision,
        zd_edition_name,
        zd_sync
    FROM {{ source('raw', 'csf_fnd_flex_value_sets') }}
),

transformed_exp_csf_fnd_flex_value_sets AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    flex_value_set_id,
    flex_value_set_name,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    validation_type,
    protected_flag,
    security_enabled_flag,
    longlist_flag,
    format_type,
    maximum_size,
    alphanumeric_allowed_flag,
    uppercase_only_flag,
    numeric_mode_enabled_flag,
    description,
    dependant_default_value,
    dependant_default_meaning,
    parent_flex_value_set_id,
    minimum_value,
    maximum_value,
    number_precision,
    zd_edition_name,
    zd_sync
    FROM source_csf_fnd_flex_value_sets
),

final AS (
    SELECT
        flex_value_set_id,
        flex_value_set_name,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        validation_type,
        protected_flag,
        security_enabled_flag,
        longlist_flag,
        format_type,
        maximum_size,
        alphanumeric_allowed_flag,
        uppercase_only_flag,
        numeric_mode_enabled_flag,
        description,
        dependant_default_value,
        dependant_default_meaning,
        parent_flex_value_set_id,
        minimum_value,
        maximum_value,
        number_precision,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_fnd_flex_value_sets
)

SELECT * FROM final