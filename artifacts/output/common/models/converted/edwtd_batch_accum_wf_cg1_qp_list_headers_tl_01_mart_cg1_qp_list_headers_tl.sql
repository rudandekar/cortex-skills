{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_qp_list_headers_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_QP_LIST_HEADERS_TL',
        'target_table': 'CG1_QP_LIST_HEADERS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.637328+00:00'
    }
) }}

WITH 

source_stg_cg1_qp_list_headers_tl AS (
    SELECT
        list_header_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        language_1,
        source_lang,
        name,
        description,
        version_no,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_qp_list_headers_tl') }}
),

source_cg1_qp_list_headers_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        list_header_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        language,
        source_lang,
        name,
        description,
        version_no
    FROM {{ source('raw', 'cg1_qp_list_headers_tl') }}
),

transformed_exp_cg1_qp_list_headers_tl AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    list_header_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    language,
    source_lang,
    name,
    description,
    version_no
    FROM source_cg1_qp_list_headers_tl
),

final AS (
    SELECT
        list_header_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        language_1,
        source_lang,
        name,
        description,
        version_no,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_qp_list_headers_tl
)

SELECT * FROM final