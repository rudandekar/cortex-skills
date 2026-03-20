{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_fnd_flex_value_sets', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_FND_FLEX_VALUE_SETS',
        'target_table': 'ST_CG1_FND_FLEX_VALUE_SETS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.943621+00:00'
    }
) }}

WITH 

source_ff_cg1_fnd_flex_value_sets AS (
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
        ges_update_date,
        global_name,
        batch_id,
        created_dt,
        action_code
    FROM {{ source('raw', 'ff_cg1_fnd_flex_value_sets') }}
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
        ges_update_date,
        global_name,
        batch_id,
        create_datetime,
        action_code
    FROM source_ff_cg1_fnd_flex_value_sets
)

SELECT * FROM final