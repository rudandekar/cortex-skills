{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_r_pl_hierarchy', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_R_PL_HIERARCHY',
        'target_table': 'MT_EIS_CA_TOTAL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.099084+00:00'
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
    FROM source_n_pl_hierarchy_node_tv
)

SELECT * FROM final