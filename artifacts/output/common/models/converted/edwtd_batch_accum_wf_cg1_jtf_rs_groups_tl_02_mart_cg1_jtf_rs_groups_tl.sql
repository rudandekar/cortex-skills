{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_jtf_rs_groups_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_JTF_RS_GROUPS_TL',
        'target_table': 'CG1_JTF_RS_GROUPS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.545504+00:00'
    }
) }}

WITH 

source_cg1_jtf_rs_groups_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        group_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        group_name,
        group_desc,
        language,
        source_lang,
        security_group_id
    FROM {{ source('raw', 'cg1_jtf_rs_groups_tl') }}
),

source_stg_cg1_jtf_rs_groups_tl AS (
    SELECT
        group_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        group_name,
        group_desc,
        language_1,
        source_lang,
        security_group_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_jtf_rs_groups_tl') }}
),

transformed_exp_cg1_jtf_rs_groups_tl AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    group_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    group_name,
    group_desc,
    language,
    source_lang,
    security_group_id
    FROM source_stg_cg1_jtf_rs_groups_tl
),

final AS (
    SELECT
        group_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        group_name,
        group_desc,
        language_1,
        source_lang,
        security_group_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_jtf_rs_groups_tl
)

SELECT * FROM final