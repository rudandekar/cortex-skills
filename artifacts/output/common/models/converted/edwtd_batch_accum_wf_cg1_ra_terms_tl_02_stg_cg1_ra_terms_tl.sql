{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_ra_terms_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_RA_TERMS_TL',
        'target_table': 'STG_CG1_RA_TERMS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.765220+00:00'
    }
) }}

WITH 

source_cg1_ra_terms_tl AS (
    SELECT
        created_by,
        creation_date,
        description,
        fully_qualified_table_name,
        language,
        last_updated_by,
        last_update_date,
        last_update_login,
        name,
        refresh_datetime,
        source_commit_time,
        source_dml_type,
        source_lang,
        term_id,
        token,
        trail_position
    FROM {{ source('raw', 'cg1_ra_terms_tl') }}
),

source_stg_cg1_ra_terms_tl AS (
    SELECT
        created_by,
        creation_date,
        description,
        cg_language,
        last_updated_by,
        last_update_date,
        last_update_login,
        name,
        source_lang,
        term_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_ra_terms_tl') }}
),

transformed_exp_cg1_ra_terms_tl AS (
    SELECT
    created_by,
    creation_date,
    description,
    language,
    last_updated_by,
    last_update_date,
    last_update_login,
    name,
    refresh_datetime,
    source_commit_time,
    source_dml_type,
    source_lang,
    term_id
    FROM source_stg_cg1_ra_terms_tl
),

final AS (
    SELECT
        created_by,
        creation_date,
        description,
        cg_language,
        last_updated_by,
        last_update_date,
        last_update_login,
        name,
        source_lang,
        term_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_ra_terms_tl
)

SELECT * FROM final