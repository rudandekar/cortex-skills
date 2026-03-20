{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ods_si_hier_published', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_ODS_SI_HIER_PUBLISHED',
        'target_table': 'FF_ODS_SI_HIER_PUBLISHED',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.074166+00:00'
    }
) }}

WITH 

source_ods_si_hier_published AS (
    SELECT
        change_type,
        created_by,
        creation_date,
        ges_global_name,
        ges_update_date,
        global_name,
        hierarchy_type,
        level_no,
        m_row$$,
        node_desc,
        node_id,
        node_type,
        node_value,
        parent_desc,
        parent_node_id,
        published_date,
        published_status,
        reference_id,
        updated_by,
        update_date
    FROM {{ source('raw', 'ods_si_hier_published') }}
),

transformed_exp_ods_si_hier_published AS (
    SELECT
    change_type,
    created_by,
    creation_date,
    ges_global_name,
    ges_update_date,
    global_name,
    hierarchy_type,
    level_no,
    m_row$$,
    node_desc,
    node_id,
    node_type,
    node_value,
    parent_desc,
    parent_node_id,
    published_date,
    published_status,
    reference_id,
    updated_by,
    update_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_ods_si_hier_published
),

final AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        ges_global_name,
        ges_update_date,
        global_name,
        hierarchy_type,
        level_no,
        node_desc,
        node_id,
        node_type,
        node_value,
        parent_desc,
        parent_node_id,
        published_date,
        published_status,
        updated_by,
        update_date,
        create_datetime,
        action_code
    FROM transformed_exp_ods_si_hier_published
)

SELECT * FROM final