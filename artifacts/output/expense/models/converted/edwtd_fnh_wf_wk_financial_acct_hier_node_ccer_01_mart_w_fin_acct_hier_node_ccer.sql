{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wk_financial_acct_hier_node_ccer', 'batch', 'edwtd_fnh'],
    meta={
        'source_workflow': 'wf_m_WK_FINANCIAL_ACCT_HIER_NODE_CCER',
        'target_table': 'W_FIN_ACCT_HIER_NODE_CCER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.008685+00:00'
    }
) }}

WITH 

source_el_et_expense_accounts AS (
    SELECT
        account_id,
        account_bucket_id,
        description,
        db_prefix
    FROM {{ source('raw', 'el_et_expense_accounts') }}
),

source_n_financial_account_tv AS (
    SELECT
        bk_financial_account_code,
        ru_bk_parent_fin_acct_code,
        account_group_id,
        start_tv_date,
        end_tv_date,
        financial_account_type_code,
        financial_account_description,
        financial_acct_start_date,
        global_account_owner_party_key,
        edw_create_datetime,
        edw_update_datetime,
        parent_role,
        edw_update_user,
        edw_create_user
    FROM {{ source('raw', 'n_financial_account_tv') }}
),

transformed_exp_m_wk_financial_acct_hier_node AS (
    SELECT
    account_group_id,
    start_tv_dt,
    end_tv_dt,
    acct_node_level_number_int,
    node_type,
    parent_account_group_id,
    action_code,
    rank_index,
    dml_type,
    account_group_descr,
    ah_reporting_sequence_num_int
    FROM source_n_financial_account_tv
),

final AS (
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
        action_code,
        dml_type,
        account_group_descr,
        ah_reporting_sequence_num_int
    FROM transformed_exp_m_wk_financial_acct_hier_node
)

SELECT * FROM final