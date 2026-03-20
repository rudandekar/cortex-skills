{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_share_hierarchy', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_SHARE_HIERARCHY',
        'target_table': 'FF_CCRM_SHARE_HIERARCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.488272+00:00'
    }
) }}

WITH 

source_ccrm_share_hierarchy AS (
    SELECT
        node_id,
        structure_node_id,
        parent_structure_node_id,
        structure_node_name,
        level_id,
        node_type
    FROM {{ source('raw', 'ccrm_share_hierarchy') }}
),

transformed_exp_ccrm_share_hierarchy AS (
    SELECT
    node_id,
    structure_node_id,
    parent_structure_node_id,
    structure_node_name,
    level_id,
    node_type,
    'Batch_Id' AS batchid_out,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_ccrm_share_hierarchy
),

final AS (
    SELECT
        batch_id,
        node_id,
        structure_node_id,
        parent_structure_node_id,
        structure_node_name,
        level_id,
        node_type,
        create_datetime,
        action_code
    FROM transformed_exp_ccrm_share_hierarchy
)

SELECT * FROM final