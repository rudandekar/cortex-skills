{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_rstd_bkgs_sls_crdt_asgmt', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_N_RSTD_BKGS_SLS_CRDT_ASGMT',
        'target_table': 'N_RSTD_BKGS_SLS_CRDT_ASGMT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.913106+00:00'
    }
) }}

WITH 

source_w_rstd_bkgs_sls_crdt_asgmt AS (
    SELECT
        rstd_bkgs_sls_crdt_asgmt_key,
        ru_sales_order_line_key,
        ru_ar_transaction_line_key,
        original_sales_territory_key,
        ru_bk_pos_trx_id_int,
        ru_bk_sales_adj_line_num_int,
        bookings_transaction_type,
        sales_territory_key,
        sales_rep_num,
        restated_sls_crdt_split_pct,
        dv_comp_us_net_price_amt,
        restatement_dt,
        src_rprtd_crdt_country_name,
        restatement_rule_name,
        default_sales_terr_level_descr,
        latest_restated_flg,
        latest_rev_restated_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        revenue_transfer_key,
        restated_sls_acct_grp_prty_key,
        so_sbscrptn_itm_sls_trx_key,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_rstd_bkgs_sls_crdt_asgmt') }}
),

final AS (
    SELECT
        rstd_bkgs_sls_crdt_asgmt_key,
        ru_sales_order_line_key,
        ru_ar_transaction_line_key,
        original_sales_territory_key,
        ru_bk_pos_trx_id_int,
        ru_bk_sales_adj_line_num_int,
        bookings_transaction_type,
        sales_territory_key,
        sales_rep_num,
        restated_sls_crdt_split_pct,
        dv_comp_us_net_price_amt,
        restatement_dt,
        src_rprtd_crdt_country_name,
        restatement_rule_name,
        default_sales_terr_level_descr,
        latest_restated_flg,
        latest_rev_restated_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        revenue_transfer_key,
        restated_sls_acct_grp_prty_key,
        so_sbscrptn_itm_sls_trx_key
    FROM source_w_rstd_bkgs_sls_crdt_asgmt
)

SELECT * FROM final