{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_xxcas_prj_hierarchy_nodes', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_HIERARCHY_NODES',
        'target_table': 'STG_CSF_XXCAS_PRJ_HIERARCHY_NODES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.690121+00:00'
    }
) }}

WITH 

source_csf_xxcas_prj_hierarchy_nodes AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        hierarchy_version_id,
        node_id,
        node_name,
        parent_node_id,
        last_update_date,
        last_updated_by,
        created_by,
        creation_date,
        last_update_login,
        ogg_key_id
    FROM {{ source('raw', 'csf_xxcas_prj_hierarchy_nodes') }}
),

source_stg_csf_xxcas_prj_hierarchy_nodes AS (
    SELECT
        hierarchy_version_id,
        node_id,
        node_name,
        parent_node_id,
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
    FROM {{ source('raw', 'stg_csf_xxcas_prj_hierarchy_nodes') }}
),

transformed_exp_csf_xxcas_prj_hierarchy_nodes AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    hierarchy_version_id,
    node_id,
    node_name,
    parent_node_id,
    last_update_date,
    last_updated_by,
    created_by,
    creation_date,
    last_update_login,
    ogg_key_id
    FROM source_stg_csf_xxcas_prj_hierarchy_nodes
),

final AS (
    SELECT
        hierarchy_version_id,
        node_id,
        node_name,
        parent_node_id,
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
    FROM transformed_exp_csf_xxcas_prj_hierarchy_nodes
)

SELECT * FROM final