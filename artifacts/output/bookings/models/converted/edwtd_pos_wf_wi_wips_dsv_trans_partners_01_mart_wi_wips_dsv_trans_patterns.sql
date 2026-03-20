{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_wips_dsv_trans_partners', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_WI_WIPS_DSV_TRANS_PARTNERS',
        'target_table': 'WI_WIPS_DSV_TRANS_PATTERNS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.588353+00:00'
    }
) }}

WITH 

source_wi_wips_dsv_trans_patterns AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        bk_end_user_wips_site_use_key
    FROM {{ source('raw', 'wi_wips_dsv_trans_patterns') }}
),

source_wi_bkg_trans AS (
    SELECT
        bk_web_order_id,
        bk_quote_num,
        cisco_created_flg,
        dsv_event_cd,
        cust_requested_ship_dtm,
        dv_cust_requested_ship_dt,
        change_identifier_txt,
        expiration_dt,
        extended_expiration_dt,
        bk_pos_transaction_id_int,
        disti_to_reseller_so_number,
        disti_so_line_number,
        disti_2_disti_trx_flag,
        distributor_reported_deal_id,
        currency_conversion_rate,
        bk_wips_originator_id_int,
        bk_ship_to_wips_site_use_key,
        bk_bill_to_wips_site_use_key,
        bk_end_user_wips_site_use_key,
        bk_sold_to_wips_site_use_key,
        bk_local_iso_currency_code,
        bk_deal_id,
        batch_id_int,
        drop_ship_flag,
        disti_rptd_cost_unit_price_amt,
        official_sale_flag,
        ru_ret_orig_bs_lst_u_price_amt,
        ru_ret_orig_cst_u_price_amt,
        professional_service_amount,
        rptd_extended_price_local_amt,
        rptd_net_unit_price_usd_amt,
        rptd_unit_price_usd_amount,
        rptd_unit_price_local_amt,
        product_net_price_amount,
        product_key,
        pos_trx_line_product_quant,
        pos_trx_line_posted_date,
        pos_trx_line_active_flag,
        transaction_date,
        ru_rslr_to_disti_po_number,
        vldtd_net_unit_price_usd_amt,
        vldtd_net_unit_price_local_amt,
        valuation_price_usd_amount,
        valuation_price_local_amount,
        distributor_warehouse_number,
        promotion_name,
        elig_for_sales_credit_type,
        edw_create_user,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        action_code,
        dml_type,
        dstrbtr_rslr_sales_order_dt,
        parent_pos_trx_id_int,
        last_source_update_dtm,
        base_list_unit_prod_price_amt,
        service_type_cd,
        service_booking_type_cd,
        service_bkg_dur_mths_cnt,
        cisco_booked_flg,
        bk_top_bkgs_pos_trx_id_int,
        line_status_cd,
        promotion_num,
        ru_bk_orig_pos_transaction_id,
        ru_ret_orig_disti_so_num,
        ru_ret_orig_disti2rslr_inv_num,
        ru_ret_orig_disti_inv_num,
        magic_key,
        start_date,
        end_date,
        duration,
        net_price_flag,
        bookings_policy_cd,
        revenue_source_code,
        gpl_price,
        gpl_price_lc,
        change_reason_desc
    FROM {{ source('raw', 'wi_bkg_trans') }}
),

final AS (
    SELECT
        bk_sold_to_wips_site_use_key,
        bk_end_user_wips_site_use_key
    FROM source_wi_bkg_trans
)

SELECT * FROM final