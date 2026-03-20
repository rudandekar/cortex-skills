{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_sm_rstd_bkgs_sls_crdt_asgmt', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_SM_RSTD_BKGS_SLS_CRDT_ASGMT',
        'target_table': 'SM_RSTD_BKGS_SLS_CRDT_ASGMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.799093+00:00'
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
        rstd_bkgs_sls_crdt_asgmt_key,
        sk_so_line_id_int,
        global_name,
        sk_customer_trx_line_id_lint,
        bk_sales_adj_line_number_int,
        bk_pos_transaction_id_int,
        share_node_id_int,
        l6_sales_territory_descr,
        sales_rep_number,
        restatement_dt,
        bookings_transaction_type,
        original_share_node_id_int,
        sk_ae_header_id_int,
        sk_ae_line_number_int,
        sk_trx_id_int,
        edw_create_dtm,
        edw_create_user
    FROM source_st_restate3_edw_writeback
)

SELECT * FROM final