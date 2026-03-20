{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sol_nrt_retro', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_WI_SOL_NRT_RETRO',
        'target_table': 'WI_SOL_NRT_RETRO_1',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.387299+00:00'
    }
) }}

WITH 

source_wi_drvd_ncr_bkg_trx_dels_sbp AS (
    SELECT
        drvd_ncr_bkg_trx_key,
        sales_credit_type_code,
        transaction_datetime,
        forward_reverse_code,
        sales_order_line_key,
        transaction_sequence_id_int,
        sales_channel_code,
        cdb_data_source_code,
        bookings_percentage,
        split_percentage,
        net_price_amount,
        transaction_quantity,
        bk_iso_currency_code,
        sales_order_key,
        es_line_seq_id_int,
        process_date,
        bk_so_upd_datetime,
        bk_sca_upd_datetime,
        sales_rep_number,
        source_system_code,
        sold_to_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        product_key,
        bk_sol_upd_datetime,
        sales_territory_key,
        sales_credit_asgn_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        so_source_commit_dt,
        sol_source_commit_dt,
        sca_source_commit_dt,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        dv_sales_order_line_key,
        dv_product_key,
        attribution_pct,
        sales_motion_cd,
        erp_annualized_bkg_trx_key,
        transactional_acv_flg,
        history_flg,
        annual_flg,
        orig_net_price_amount,
        dv_running_total_local_amt,
        order_value_amount,
        bookings_policy_cd,
        source_rep_annual_trxl_amt,
        unit_whlsl_prc_lst_loc_amt,
        buying_transaction_type_name,
        linked_bookings_measure_key
    FROM {{ source('raw', 'wi_drvd_ncr_bkg_trx_dels_sbp') }}
),

source_wi_nsoln_nrt_retro AS (
    SELECT
        sales_order_line_key,
        bk_inventory_organization_key,
        dd_bk_inv_organization_name_cd
    FROM {{ source('raw', 'wi_nsoln_nrt_retro') }}
),

source_wi_sol_nrt_retro_1 AS (
    SELECT
        sales_order_line_key,
        ep_header_id_int,
        sales_order_key,
        ss_cd,
        product_key,
        ep_product_inv_item_id_int,
        ship_to_customer_key,
        ep_stc_ship_to_org_id_int,
        so_line_source_update_dtm,
        source_commit_dtm
    FROM {{ source('raw', 'wi_sol_nrt_retro_1') }}
),

source_wi_sol_nrt_retro_2 AS (
    SELECT
        sales_order_line_key,
        ep_header_id_int,
        sales_order_key,
        new_sales_order_key,
        ss_cd,
        product_key,
        new_product_key,
        ep_product_inv_item_id_int,
        ship_to_customer_key,
        new_ship_to_customer_key,
        ep_stc_ship_to_org_id_int,
        so_line_source_update_dtm,
        source_commit_dtm
    FROM {{ source('raw', 'wi_sol_nrt_retro_2') }}
),

source_n_sales_order_line_nrt_hist_tv AS (
    SELECT
        sales_order_line_key,
        ep_header_id_int,
        sales_order_key,
        order_qty,
        unit_net_price_local_amt,
        bookings_pct,
        sk_sales_order_line_id_int,
        ss_cd,
        product_key,
        ep_product_inv_item_id_int,
        ship_to_customer_key,
        ep_stc_ship_to_org_id_int,
        so_line_source_update_dtm,
        dv_so_line_source_update_dt,
        source_commit_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dtm,
        end_tv_dtm,
        unit_whlsl_prc_lst_loc_amt
    FROM {{ source('raw', 'n_sales_order_line_nrt_hist_tv') }}
),

final AS (
    SELECT
        sales_order_line_key,
        ep_header_id_int,
        sales_order_key,
        ss_cd,
        product_key,
        ep_product_inv_item_id_int,
        ship_to_customer_key,
        ep_stc_ship_to_org_id_int,
        so_line_source_update_dtm,
        source_commit_dtm
    FROM source_n_sales_order_line_nrt_hist_tv
)

SELECT * FROM final