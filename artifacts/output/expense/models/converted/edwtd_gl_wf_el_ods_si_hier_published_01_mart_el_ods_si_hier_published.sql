{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ods_si_hier_published', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_ODS_SI_HIER_PUBLISHED',
        'target_table': 'EL_ODS_SI_HIER_PUBLISHED',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.676002+00:00'
    }
) }}

WITH 

source_st_ods_si_hier_published AS (
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
    FROM {{ source('raw', 'st_ods_si_hier_published') }}
),

final AS (
    SELECT
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
        update_date
    FROM source_st_ods_si_hier_published
)

SELECT * FROM final