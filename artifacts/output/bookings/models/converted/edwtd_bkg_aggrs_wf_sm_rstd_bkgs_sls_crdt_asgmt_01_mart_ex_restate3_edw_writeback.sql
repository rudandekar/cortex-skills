{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_rstd_bkgs_sls_crdt_asgmt', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_SM_RSTD_BKGS_SLS_CRDT_ASGMT',
        'target_table': 'EX_RESTATE3_EDW_WRITEBACK',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.402411+00:00'
    }
) }}

WITH 

source_ex_restate3_edw_writeback AS (
    SELECT
        line_id,
        pos_trans_id,
        sales_adj_line_num,
        cust_trx_line_id,
        book_source,
        global_name,
        restate_am_id,
        edw_node_id,
        split_perc,
        book_net,
        credit_country,
        rule_name,
        default_level,
        restatement_date,
        level_6,
        exception_type,
        rte_header_id,
        rte_line_number,
        restate_sav_id,
        tsl_trx_id,
        original_share_node_id
    FROM {{ source('raw', 'ex_restate3_edw_writeback') }}
),

source_st_restate3_edw_writeback AS (
    SELECT
        line_id,
        pos_trans_id,
        sales_adj_line_num,
        cust_trx_line_id,
        book_source,
        global_name,
        restate_am_id,
        edw_node_id,
        split_perc,
        book_net,
        credit_country,
        rule_name,
        default_level,
        restatement_date,
        level_6,
        rte_header_id,
        rte_line_number,
        restate_sav_id,
        tsl_trx_id,
        original_share_node_id
    FROM {{ source('raw', 'st_restate3_edw_writeback') }}
),

final AS (
    SELECT
        line_id,
        pos_trans_id,
        sales_adj_line_num,
        cust_trx_line_id,
        book_source,
        global_name,
        restate_am_id,
        edw_node_id,
        split_perc,
        book_net,
        credit_country,
        rule_name,
        default_level,
        restatement_date,
        level_6,
        exception_type,
        rte_header_id,
        rte_line_number,
        restate_sav_id,
        tsl_trx_id,
        original_share_node_id
    FROM source_st_restate3_edw_writeback
)

SELECT * FROM final