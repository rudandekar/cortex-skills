{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_fin_rli_hierarchy', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_MT_FIN_RLI_HIERARCHY',
        'target_table': 'MT_FIN_RLI_HIERARCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.687593+00:00'
    }
) }}

WITH 

source_n_fin_report_line_item_node AS (
    SELECT
        bk_finance_report_line_item_cd,
        finance_report_line_item_descr,
        ru_prnt_fin_report_ln_item_cd,
        dv_level_num_int,
        line_item_node_type,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_fin_report_line_item_node') }}
),

final AS (
    SELECT
        bk_finance_report_line_item_cd,
        node_level01_item_cd,
        node_level01_item_descr,
        node_level01_item_seq_num_int,
        node_level02_item_cd,
        node_level02_item_descr,
        node_level02_item_seq_num_int,
        node_level03_item_cd,
        node_level03_item_descr,
        node_level03_item_seq_num_int,
        node_level04_item_cd,
        node_level04_item_descr,
        node_level04_item_seq_num_int,
        node_level05_item_cd,
        node_level05_item_descr,
        node_level05_item_seq_num_int,
        node_level06_item_cd,
        node_level06_item_descr,
        node_level06_item_seq_num_int,
        node_level07_item_cd,
        node_level07_item_descr,
        node_level07_item_seq_num_int,
        node_level08_item_cd,
        node_level08_item_descr,
        node_level08_item_seq_num_int,
        node_level09_item_cd,
        node_level09_item_descr,
        node_level09_item_seq_num_int,
        node_level10_item_cd,
        node_level10_item_descr,
        node_level10_item_seq_num_int,
        node_level11_item_cd,
        node_level11_item_descr,
        node_level11_item_seq_num_int,
        node_level12_item_cd,
        node_level12_item_descr,
        node_level12_item_seq_num_int,
        node_level13_item_cd,
        node_level13_item_descr,
        node_level13_item_seq_num_int,
        node_level14_item_cd,
        node_level14_item_descr,
        node_level14_item_seq_num_int,
        node_level15_item_cd,
        node_level15_item_descr,
        node_level15_item_seq_num_int,
        lowest_level,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_n_fin_report_line_item_node
)

SELECT * FROM final