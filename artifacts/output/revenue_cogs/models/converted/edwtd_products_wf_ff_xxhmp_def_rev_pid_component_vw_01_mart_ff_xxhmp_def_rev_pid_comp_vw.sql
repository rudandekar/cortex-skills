{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_xxhmp_def_rev_pid_component_vw', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_FF_XXHMP_DEF_REV_PID_COMPONENT_VW',
        'target_table': 'FF_XXHMP_DEF_REV_PID_COMP_VW',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.718180+00:00'
    }
) }}

WITH 

source_xxhmp_def_rev_pid_component_vw AS (
    SELECT
        change_id,
        node_id,
        product_id,
        node_type,
        category_name,
        event_name,
        relationship_start_date,
        relationship_end_date,
        source_update_date
    FROM {{ source('raw', 'xxhmp_def_rev_pid_component_vw') }}
),

final AS (
    SELECT
        change_id,
        node_id,
        product_id,
        node_type,
        category_name,
        event_name,
        relationship_start_date,
        relationship_end_date,
        source_update_date
    FROM source_xxhmp_def_rev_pid_component_vw
)

SELECT * FROM final