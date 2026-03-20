{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_dfrd_rev_offr_hier_cmpnt_prdt', 'batch', 'edwtd_products'],
    meta={
        'source_workflow': 'wf_m_W_DFRD_REV_OFFR_HIER_CMPNT_PRDT',
        'target_table': 'WI_XXHMP_DEF_REV_PID_COMPNT_VW',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.357891+00:00'
    }
) }}

WITH 

source_w_dfrd_rev_offr_hier_cmpnt_prdt AS (
    SELECT
        product_key,
        dfrd_rev_offr_hier_node_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_dfrd_rev_offr_hier_cmpnt_prdt') }}
),

source_wi_xxhmp_def_rev_pid_compnt_vw AS (
    SELECT
        node_id,
        product_id,
        relationship_start_date,
        relationship_end_date,
        source_update_date
    FROM {{ source('raw', 'wi_xxhmp_def_rev_pid_compnt_vw') }}
),

source_ex_xxhmp_def_rev_pid_compnt_vw AS (
    SELECT
        node_id,
        product_id,
        relationship_start_date,
        relationship_end_date,
        source_update_date,
        exception_type
    FROM {{ source('raw', 'ex_xxhmp_def_rev_pid_compnt_vw') }}
),

final AS (
    SELECT
        node_id,
        product_id,
        relationship_start_date,
        relationship_end_date,
        source_update_date
    FROM source_ex_xxhmp_def_rev_pid_compnt_vw
)

SELECT * FROM final