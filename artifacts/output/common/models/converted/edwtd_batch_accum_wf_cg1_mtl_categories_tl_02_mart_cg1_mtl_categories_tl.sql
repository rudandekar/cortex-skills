{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_mtl_categories_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_MTL_CATEGORIES_TL',
        'target_table': 'CG1_MTL_CATEGORIES_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.642323+00:00'
    }
) }}

WITH 

source_stg_cg1_mtl_categories_tl AS (
    SELECT
        category_id,
        language_code,
        source_lang,
        description,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_mtl_categories_tl') }}
),

source_cg1_mtl_categories_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        category_id,
        language,
        source_lang,
        description,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login
    FROM {{ source('raw', 'cg1_mtl_categories_tl') }}
),

transformed_exp_cg1_mtl_categories_tl AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    category_id,
    language,
    source_lang,
    description,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login
    FROM source_cg1_mtl_categories_tl
),

final AS (
    SELECT
        category_id,
        language_code,
        source_lang,
        description,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_mtl_categories_tl
)

SELECT * FROM final