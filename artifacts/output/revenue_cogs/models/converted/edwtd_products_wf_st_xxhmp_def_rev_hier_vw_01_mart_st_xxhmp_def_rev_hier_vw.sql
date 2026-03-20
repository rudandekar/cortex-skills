{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_xxhmp_def_rev_hier_vw', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_ST_XXHMP_DEF_REV_HIER_VW',
        'target_table': 'ST_XXHMP_DEF_REV_HIER_VW',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.085283+00:00'
    }
) }}

WITH 

source_ff_xxhmp_def_rev_hier_vw AS (
    SELECT
        change_id,
        node_id,
        parent_node_id,
        node_name,
        node_type,
        category_name,
        event_name,
        relationship_start_date,
        relationship_end_date,
        source_update_date
    FROM {{ source('raw', 'ff_xxhmp_def_rev_hier_vw') }}
),

final AS (
    SELECT
        change_id,
        node_id,
        parent_node_id,
        node_name,
        node_type,
        category_name,
        event_name,
        relationship_start_date,
        relationship_end_date,
        source_update_date
    FROM source_ff_xxhmp_def_rev_hier_vw
)

SELECT * FROM final