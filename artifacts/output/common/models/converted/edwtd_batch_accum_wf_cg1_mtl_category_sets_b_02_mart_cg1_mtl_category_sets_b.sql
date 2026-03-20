{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_mtl_category_sets_b', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_MTL_CATEGORY_SETS_B',
        'target_table': 'CG1_MTL_CATEGORY_SETS_B',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.906604+00:00'
    }
) }}

WITH 

source_cg1_mtl_category_sets_b AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        category_set_id,
        structure_id,
        validate_flag,
        control_level,
        default_category_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        mult_item_cat_assign_flag,
        control_level_updateable_flag,
        mult_item_cat_updateable_flag,
        hierarchy_enabled,
        validate_flag_updateable_flag,
        user_creation_allowed_flag,
        raise_item_cat_assign_event,
        raise_alt_cat_hier_chg_event,
        raise_catalog_cat_chg_event
    FROM {{ source('raw', 'cg1_mtl_category_sets_b') }}
),

source_stg_cg1_mtl_category_sets_b AS (
    SELECT
        category_set_id,
        structure_id,
        validate_flag,
        control_level,
        default_category_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        mult_item_cat_assign_flag,
        control_level_updateable_flag,
        mult_item_cat_updateable_flag,
        hierarchy_enabled,
        validate_flag_updateable_flag,
        user_creation_allowed_flag,
        raise_item_cat_assign_event,
        raise_alt_cat_hier_chg_event,
        raise_catalog_cat_chg_event,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_mtl_category_sets_b') }}
),

transformed_exp_cg1_mtl_category_sets_b AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    category_set_id,
    structure_id,
    validate_flag,
    control_level,
    default_category_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    mult_item_cat_assign_flag,
    control_level_updateable_flag,
    mult_item_cat_updateable_flag,
    hierarchy_enabled,
    validate_flag_updateable_flag,
    user_creation_allowed_flag,
    raise_item_cat_assign_event,
    raise_alt_cat_hier_chg_event,
    raise_catalog_cat_chg_event
    FROM source_stg_cg1_mtl_category_sets_b
),

final AS (
    SELECT
        category_set_id,
        structure_id,
        validate_flag,
        control_level,
        default_category_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        mult_item_cat_assign_flag,
        control_level_updateable_flag,
        mult_item_cat_updateable_flag,
        hierarchy_enabled,
        validate_flag_updateable_flag,
        user_creation_allowed_flag,
        raise_item_cat_assign_event,
        raise_alt_cat_hier_chg_event,
        raise_catalog_cat_chg_event,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_mtl_category_sets_b
)

SELECT * FROM final