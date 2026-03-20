{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_r_financial_account_hierarchy_ccer', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_R_FINANCIAL_ACCOUNT_HIERARCHY_CCER',
        'target_table': 'R_FINANCIAL_ACCOUNT_HIERARCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.641560+00:00'
    }
) }}

WITH 

source_n_financial_acct_hier_node_tv AS (
    SELECT
        account_group_id,
        start_tv_dt,
        end_tv_dt,
        acct_node_level_number_int,
        node_type,
        parent_account_group_id,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        account_group_descr
    FROM {{ source('raw', 'n_financial_acct_hier_node_tv') }}
),

final AS (
    SELECT
        bk_financial_account_code,
        level01_acct_group_id,
        level01_account_group_descr,
        node_level01_seq_num_int,
        level02_acct_group_id,
        level02_account_group_descr,
        node_level02_seq_num_int,
        level03_acct_group_id,
        level03_account_group_descr,
        node_level03_seq_num_int,
        level04_acct_group_id,
        level04_account_group_descr,
        node_level04_seq_num_int,
        level05_acct_group_id,
        level05_account_group_descr,
        node_level05_seq_num_int,
        level06_acct_group_id,
        level06_account_group_descr,
        node_level06_seq_num_int,
        level07_acct_group_id,
        level07_account_group_descr,
        node_level07_seq_num_int,
        level08_acct_group_id,
        level08_account_group_descr,
        node_level08_seq_num_int,
        level09_acct_group_id,
        level09_account_group_descr,
        node_level09_seq_num_int,
        level10_acct_group_id,
        level10_account_group_descr,
        node_level10_seq_num_int,
        level11_acct_group_id,
        level11_account_group_descr,
        node_level11_seq_num_int,
        level12_acct_group_id,
        level12_account_group_descr,
        node_level12_seq_num_int,
        level13_acct_group_id,
        level13_account_group_descr,
        node_level13_seq_num_int,
        level14_acct_group_id,
        level14_account_group_descr,
        node_level14_seq_num_int,
        level15_acct_group_id,
        level15_account_group_descr,
        node_level15_seq_num_int,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_n_financial_acct_hier_node_tv
)

SELECT * FROM final