{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_si_hierarchy_ah_published', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_SI_HIERARCHY_AH_PUBLISHED',
        'target_table': 'EL_SI_HIERARCHY_AH_PUBLISHED',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.093084+00:00'
    }
) }}

WITH 

source_st_si_hierarchy_ah_published AS (
    SELECT
        batch_id,
        node_id,
        global_name,
        level_no,
        node_type,
        node_value,
        node_desc,
        parent_desc,
        parent_node_id,
        hierarchy_type,
        creation_date,
        created_by,
        update_date,
        updated_by,
        reference_id,
        published_date,
        published_status,
        change_type,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_hierarchy_ah_published') }}
),

final AS (
    SELECT
        node_id,
        global_name,
        level_no,
        node_type,
        node_value,
        node_desc,
        parent_desc,
        parent_node_id,
        hierarchy_type,
        creation_date,
        created_by,
        update_date,
        updated_by,
        reference_id,
        published_date,
        published_status,
        change_type,
        create_datetime
    FROM source_st_si_hierarchy_ah_published
)

SELECT * FROM final