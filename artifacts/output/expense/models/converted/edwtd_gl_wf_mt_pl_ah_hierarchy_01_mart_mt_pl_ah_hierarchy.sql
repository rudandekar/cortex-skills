{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_pl_ah_hierarchy', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_MT_PL_AH_HIERARCHY',
        'target_table': 'MT_PL_AH_HIERARCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.899362+00:00'
    }
) }}

WITH 

source_n_ah_pl_hierarchy_node_h1 AS (
    SELECT
        pl_hierarchy_node_key,
        pl_hier_node_level_number_int,
        parent_pl_hier_node_key,
        pl_hierarchy_node_nm,
        node_value_cd,
        node_type,
        node_id_int,
        bk_profit_center_name,
        bk_fd_alternate_hierarchy_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_ah_pl_hierarchy_node_h1') }}
),

final AS (
    SELECT
        financial_department_code,
        financial_company_code,
        node_level01_value,
        node_level01_name,
        node_level02_value,
        node_level02_name,
        node_level03_value,
        node_level03_name,
        node_level04_value,
        node_level04_name,
        node_level05_value,
        node_level05_name,
        node_level06_value,
        node_level06_name,
        node_level07_value,
        node_level07_name,
        node_level08_value,
        node_level08_name,
        node_level09_value,
        node_level09_name,
        node_level10_value,
        node_level10_name,
        node_level11_value,
        node_level11_name,
        node_level12_value,
        node_level12_name,
        node_level13_value,
        node_level13_name,
        node_level14_value,
        node_level14_name,
        node_level15_value,
        node_level15_name,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        node_level01_id_int,
        node_level02_id_int,
        node_level03_id_int,
        node_level04_id_int,
        node_level05_id_int,
        node_level06_id_int,
        node_level07_id_int,
        node_level08_id_int,
        node_level09_id_int,
        node_level10_id_int,
        node_level11_id_int,
        node_level12_id_int,
        node_level13_id_int,
        node_level14_id_int,
        node_level15_id_int
    FROM source_n_ah_pl_hierarchy_node_h1
)

SELECT * FROM final