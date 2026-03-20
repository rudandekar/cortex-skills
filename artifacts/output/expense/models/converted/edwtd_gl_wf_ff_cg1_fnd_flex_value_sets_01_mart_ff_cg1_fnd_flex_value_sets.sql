{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_cg1_fnd_flex_value_sets', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_CG1_FND_FLEX_VALUE_SETS',
        'target_table': 'FF_CG1_FND_FLEX_VALUE_SETS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.754637+00:00'
    }
) }}

WITH 

source_cg1_fnd_flex_value_sets AS (
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
        global_name
    FROM {{ source('raw', 'cg1_fnd_flex_value_sets') }}
),

transformed_exp_cg1_fnd_flex_value_sets AS (
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
    'BatchId' AS o_batch_id,
    CURRENT_TIMESTAMP() AS o_created_datetime,
    'I' AS o_action_code
    FROM source_cg1_fnd_flex_value_sets
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
        created_dt,
        action_code
    FROM transformed_exp_cg1_fnd_flex_value_sets
)

SELECT * FROM final