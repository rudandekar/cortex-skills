{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_pos_trx_ln_to_sol_link_tv', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_N_POS_TRX_LN_TO_SOL_LINK_TV',
        'target_table': 'N_POS_TRX_LN_TO_SOL_LINK_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.932872+00:00'
    }
) }}

WITH 

source_n_pos_trx_ln_to_sol_link AS (
    SELECT
        sales_order_line_key,
        src_reported_rev_src_cd,
        service_quote_line_key,
        sk_unique_id_int,
        dsv_bookings_flg,
        active_flg,
        crtd_tagging_rule_process_nm,
        upd_tagging_rule_process_nm,
        src_reported_quote_num,
        src_reported_contract_num,
        matching_method_for_tagging_nm,
        src_reported_sls_ord_header_id,
        src_reported_quote_header_id,
        dv_split_allocation_pct,
        ss_code,
        bk_pos_transaction_id_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_pos_trx_ln_to_sol_link') }}
),

source_n_pos_trx_ln_to_sol_link_tv AS (
    SELECT
        sales_order_line_key,
        src_reported_rev_src_cd,
        service_quote_line_key,
        sk_unique_id_int,
        dsv_bookings_flg,
        active_flg,
        crtd_tagging_rule_process_nm,
        upd_tagging_rule_process_nm,
        src_reported_quote_num,
        src_reported_contract_num,
        matching_method_for_tagging_nm,
        src_reported_sls_ord_header_id,
        src_reported_quote_header_id,
        dv_split_allocation_pct,
        ss_code,
        bk_pos_transaction_id_int,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_pos_trx_ln_to_sol_link_tv') }}
),

final AS (
    SELECT
        sales_order_line_key,
        src_reported_rev_src_cd,
        service_quote_line_key,
        sk_unique_id_int,
        dsv_bookings_flg,
        active_flg,
        crtd_tagging_rule_process_nm,
        upd_tagging_rule_process_nm,
        src_reported_quote_num,
        src_reported_contract_num,
        matching_method_for_tagging_nm,
        src_reported_sls_ord_header_id,
        src_reported_quote_header_id,
        dv_split_allocation_pct,
        ss_code,
        bk_pos_transaction_id_int,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_n_pos_trx_ln_to_sol_link_tv
)

SELECT * FROM final