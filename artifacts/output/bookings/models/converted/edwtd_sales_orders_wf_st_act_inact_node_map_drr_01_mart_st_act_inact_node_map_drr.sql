{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_act_inact_node_map_drr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_ST_ACT_INACT_NODE_MAP_DRR',
        'target_table': 'ST_ACT_INACT_NODE_MAP_DRR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.793107+00:00'
    }
) }}

WITH 

source_ff_act_inact_node_map_drr AS (
    SELECT
        node_attr_dtl_id,
        node_id,
        replaced_node_id,
        effective_date,
        expiration_date,
        date_created,
        date_modified,
        version_id
    FROM {{ source('raw', 'ff_act_inact_node_map_drr') }}
),

final AS (
    SELECT
        node_attr_dtl_id,
        node_id,
        replaced_node_id,
        effective_date,
        expiration_date,
        date_created,
        date_modified,
        version_id
    FROM source_ff_act_inact_node_map_drr
)

SELECT * FROM final