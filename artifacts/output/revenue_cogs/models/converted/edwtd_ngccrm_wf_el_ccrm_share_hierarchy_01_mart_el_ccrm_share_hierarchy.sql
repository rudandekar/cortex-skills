{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccrm_share_hierarchy', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_EL_CCRM_SHARE_HIERARCHY',
        'target_table': 'EL_CCRM_SHARE_HIERARCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.342213+00:00'
    }
) }}

WITH 

source_st_ccrm_share_hierarchy AS (
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
    FROM {{ source('raw', 'st_ccrm_share_hierarchy') }}
),

final AS (
    SELECT
        node_id,
        structure_node_name,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date
    FROM source_st_ccrm_share_hierarchy
)

SELECT * FROM final