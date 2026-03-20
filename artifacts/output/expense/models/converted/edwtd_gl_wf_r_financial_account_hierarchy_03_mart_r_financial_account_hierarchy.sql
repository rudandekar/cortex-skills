{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_r_financial_account_hierarchy', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_R_FINANCIAL_ACCOUNT_HIERARCHY',
        'target_table': 'R_FINANCIAL_ACCOUNT_HIERARCHY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.786619+00:00'
    }
) }}

WITH 

source_n_financial_account_tv AS (
    SELECT
        bk_financial_account_code,
        ru_bk_parent_fin_acct_code,
        account_group_id,
        start_tv_date,
        end_tv_date,
        financial_account_type,
        financial_account_description,
        financial_acct_start_date,
        global_account_owner_party_key,
        edw_create_datetime,
        edw_update_datetime,
        parent_role,
        edw_update_user,
        edw_create_user,
        financial_acct_display_seq_int,
        controllable_account_flg,
        gps_spend_category_cd,
        gps_spend_sub_category_cd
    FROM {{ source('raw', 'n_financial_account_tv') }}
),

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
        edw_update_dtm
    FROM {{ source('raw', 'n_financial_acct_hier_node_tv') }}
),

final AS (
    SELECT
        bk_financial_account_code,
        level01_acct_group_id,
        level02_acct_group_id,
        level03_acct_group_id,
        level04_acct_group_id,
        level05_acct_group_id,
        level06_acct_group_id,
        level07_acct_group_id,
        level08_acct_group_id,
        level09_acct_group_id,
        level10_acct_group_id,
        level11_acct_group_id,
        level12_acct_group_id,
        level13_acct_group_id,
        level14_acct_group_id,
        level15_acct_group_id,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm
    FROM source_n_financial_acct_hier_node_tv
)

SELECT * FROM final