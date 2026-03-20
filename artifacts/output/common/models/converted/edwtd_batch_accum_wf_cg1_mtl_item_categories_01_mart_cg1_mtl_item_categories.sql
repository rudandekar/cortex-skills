{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_mtl_item_categories', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_MTL_ITEM_CATEGORIES',
        'target_table': 'CG1_MTL_ITEM_CATEGORIES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.717960+00:00'
    }
) }}

WITH 

source_cg1_mtl_item_categories AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        inventory_item_id,
        organization_id,
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
        wh_update_date
    FROM {{ source('raw', 'cg1_mtl_item_categories') }}
),

source_stg_cg1_mtl_item_categories AS (
    SELECT
        inventory_item_id,
        organization_id,
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
        wh_update_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_mtl_item_categories') }}
),

transformed_exp_cg1_mtl_item_categories AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    inventory_item_id,
    organization_id,
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
    wh_update_date
    FROM source_stg_cg1_mtl_item_categories
),

final AS (
    SELECT
        inventory_item_id,
        organization_id,
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
        wh_update_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_mtl_item_categories
)

SELECT * FROM final