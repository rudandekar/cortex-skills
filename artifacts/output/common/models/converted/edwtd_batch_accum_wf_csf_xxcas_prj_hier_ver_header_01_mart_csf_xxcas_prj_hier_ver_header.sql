{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_xxcas_prj_hier_ver_header', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_HIER_VER_HEADER',
        'target_table': 'CSF_XXCAS_PRJ_HIER_VER_HEADER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.111510+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_prj_hier_ver_header AS (
    SELECT
        hierarchy_version_id,
        hierarchy_name,
        hierarchy_version_name,
        status,
        initial_import_date,
        initial_import_by,
        last_validated_date,
        last_validated_by,
        final_processed_date,
        final_processed_by,
        archived_date,
        archived_by,
        last_update_date,
        last_updated_by,
        created_by,
        creation_date,
        last_update_login,
        ogg_key_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_hier_ver_header') }}
),

source_csf_xxcas_prj_hier_version_hdr AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        hierarchy_version_id,
        hierarchy_name,
        hierarchy_version_name,
        status,
        initial_import_date,
        initial_import_by,
        last_validated_date,
        last_validated_by,
        final_processed_date,
        final_processed_by,
        archived_date,
        archived_by,
        last_update_date,
        last_updated_by,
        created_by,
        creation_date,
        last_update_login,
        ogg_key_id,
        load_id
    FROM {{ source('raw', 'csf_xxcas_prj_hier_version_hdr') }}
),

transformed_exp_csf_xxcas_prj_hier_version_hdr AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    hierarchy_version_id,
    hierarchy_name,
    hierarchy_version_name,
    status,
    initial_import_date,
    initial_import_by,
    last_validated_date,
    last_validated_by,
    final_processed_date,
    final_processed_by,
    archived_date,
    archived_by,
    last_update_date,
    last_updated_by,
    created_by,
    creation_date,
    last_update_login,
    ogg_key_id,
    load_id
    FROM source_csf_xxcas_prj_hier_version_hdr
),

final AS (
    SELECT
        hierarchy_version_id,
        hierarchy_name,
        hierarchy_version_name,
        status,
        initial_import_date,
        initial_import_by,
        last_validated_date,
        last_validated_by,
        final_processed_date,
        final_processed_by,
        archived_date,
        archived_by,
        last_update_date,
        last_updated_by,
        created_by,
        creation_date,
        last_update_login,
        ogg_key_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_hier_version_hdr
)

SELECT * FROM final