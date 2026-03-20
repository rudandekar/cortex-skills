{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_share_hierarchy', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_SHARE_HIERARCHY',
        'target_table': 'ST_CCRM_SHARE_HIERARCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.662149+00:00'
    }
) }}

WITH 

source_ff_ccrm_share_hierarchy AS (
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
    FROM {{ source('raw', 'ff_ccrm_share_hierarchy') }}
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
    FROM source_ff_ccrm_share_hierarchy
)

SELECT * FROM final