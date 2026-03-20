{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_r_pl_hierarchy', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_R_PL_HIERARCHY',
        'target_table': 'R_PL_HIERARCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.895480+00:00'
    }
) }}

WITH 

source_n_fin_bi_prft_cntr_app_spcfc AS (
    SELECT
        bk_profit_center_name,
        display_sequence_num_int,
        ru_parent_profit_center_name,
        with_parent_role,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_fin_bi_prft_cntr_app_spcfc') }}
),

source_mt_eis_ca_total AS (
    SELECT
        company_cd,
        department_cd,
        level_1_display_seq_int,
        level_1_profit_center_name,
        level_2_display_seq_int,
        level_2_profit_center_name,
        level_3_display_seq_int,
        level_3_profit_center_name,
        level_4_display_seq_int,
        level_4_profit_center_name
    FROM {{ source('raw', 'mt_eis_ca_total') }}
),

source_n_pl_hierarchy_node_tv AS (
    SELECT
        pl_hierarchy_node_key,
        start_tv_dt,
        end_tv_dt,
        node_type,
        node_value_cd,
        parent_pl_hier_node_key,
        pl_hier_node_level_number_int,
        pl_hierarchy_node_nm,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        node_id_int
    FROM {{ source('raw', 'n_pl_hierarchy_node_tv') }}
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
    FROM source_n_pl_hierarchy_node_tv
)

SELECT * FROM final