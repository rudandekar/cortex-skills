{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_dfrd_rev_offr_hier_node', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_W_DFRD_REV_OFFR_HIER_NODE',
        'target_table': 'W_DFRD_REV_OFFR_HIER_NODE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.355234+00:00'
    }
) }}

WITH 

source_w_dfrd_rev_offr_hier_node AS (
    SELECT
        dfrd_rev_offr_hier_node_key,
        dfrd_rev_offr_hier_group_node_key,
        sk_node_id_int,
        node_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_dfrd_rev_offr_hier_node') }}
),

source_wi_xxhmp_def_rev_hier_vw AS (
    SELECT
        node_id,
        parent_node_id,
        node_name,
        relationship_start_date,
        relationship_end_date,
        source_update_date
    FROM {{ source('raw', 'wi_xxhmp_def_rev_hier_vw') }}
),

source_sm_dfrd_rev_offr_hier_node AS (
    SELECT
        dfrd_rev_offr_hier_node_key,
        sk_node_id_int,
        edw_create_dtm,
        edw_create_user
    FROM {{ source('raw', 'sm_dfrd_rev_offr_hier_node') }}
),

final AS (
    SELECT
        dfrd_rev_offr_hier_node_key,
        dfrd_rev_offr_hier_group_node_key,
        sk_node_id_int,
        node_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM source_sm_dfrd_rev_offr_hier_node
)

SELECT * FROM final