{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ods_si_hier_published_dels', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_ODS_SI_HIER_PUBLISHED_DELS',
        'target_table': 'ST_ODS_SI_HIER_PUBLISHED_DELS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.996421+00:00'
    }
) }}

WITH 

source_ff_ods_si_hier_published_dels AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        ges_delete_date,
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
    FROM {{ source('raw', 'ff_ods_si_hier_published_dels') }}
),

final AS (
    SELECT
        batch_id,
        created_by,
        creation_date,
        ges_delete_date,
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
    FROM source_ff_ods_si_hier_published_dels
)

SELECT * FROM final