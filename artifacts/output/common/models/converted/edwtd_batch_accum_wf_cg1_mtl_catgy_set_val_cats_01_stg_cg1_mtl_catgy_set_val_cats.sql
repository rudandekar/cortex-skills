{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_mtl_catgy_set_val_cats', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_MTL_CATGY_SET_VAL_CATS',
        'target_table': 'STG_CG1_MTL_CATGY_SET_VAL_CATS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.689780+00:00'
    }
) }}

WITH 

source_stg_cg1_mtl_catgy_set_val_cats AS (
    SELECT
        category_set_id,
        category_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        parent_category_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_mtl_catgy_set_val_cats') }}
),

source_cg1_mtl_catg_set_valid_cats AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        category_set_id,
        category_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        parent_category_id
    FROM {{ source('raw', 'cg1_mtl_catg_set_valid_cats') }}
),

transformed_exp_cg1_mtl_catgy_set_val_cats AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    category_set_id,
    category_id,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login,
    request_id,
    program_application_id,
    program_id,
    program_update_date,
    parent_category_id
    FROM source_cg1_mtl_catg_set_valid_cats
),

final AS (
    SELECT
        category_set_id,
        category_id,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        request_id,
        program_application_id,
        program_id,
        program_update_date,
        parent_category_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_mtl_catgy_set_val_cats
)

SELECT * FROM final