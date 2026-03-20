{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_financial_acct_hier_node_tv', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_N_FINANCIAL_ACCT_HIER_NODE_TV',
        'target_table': 'N_FINANCIAL_ACCT_HIER_NODE_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.844903+00:00'
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
        edw_update_dtm
    FROM {{ source('raw', 'n_financial_acct_hier_node_tv') }}
),

source_w_financial_acct_hier_node AS (
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
        dml_type
    FROM {{ source('raw', 'w_financial_acct_hier_node') }}
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
        edw_update_dtm
    FROM source_w_financial_acct_hier_node
)

SELECT * FROM final