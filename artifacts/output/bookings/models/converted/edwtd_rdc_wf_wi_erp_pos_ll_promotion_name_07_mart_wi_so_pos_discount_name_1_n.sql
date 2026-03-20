{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_erp_pos_ll_promotion_name', 'batch', 'edwtd_rdc'],
    meta={
        'source_workflow': 'wf_m_WI_ERP_POS_LL_PROMOTION_NAME',
        'target_table': 'WI_SO_POS_DISCOUNT_NAME_1_N',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.875552+00:00'
    }
) }}

WITH 

source_n_deal_quote_line_concession AS (
    SELECT
        bk_deal_qt_ln_concession_key,
        bk_deal_quote_line_key,
        bk_quote_num,
        bk_deal_concession_source_num,
        concession_percent,
        concession_usd_amt,
        concession_expiration_dt,
        discount_name,
        requested_non_standard_discoun,
        approved_non_standard_discount,
        adjustment_price_amt,
        source_reported_promotion_cd,
        source_reported_promotion_name,
        applied_product_family_id,
        sk_concession_object_id_int,
        ru_bk_rebate_trx_type_cd,
        ru_bk_rebate_payment_system_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_concession_type_name,
        concession_applied_flg,
        manually_applied_discount_flg,
        pricing_scenario_ln_key,
        src_rptd_channel_program_name,
        src_rptd_promotion_rev_num,
        discount_method_cd,
        src_rptd_discount_per_qty_amt,
        dv_discount_per_qty_pct,
        dv_dscnt_perqty_lmpsum_usd_amt,
        adjusted_selling_price_usd_amt,
        src_updated_dtm,
        dv_src_updated_dt,
        src_creation_dtm,
        dv_src_creation_dt
    FROM {{ source('raw', 'n_deal_quote_line_concession') }}
),

source_wi_so_pos_ll_promo_name_int1 AS (
    SELECT
        bk_deal_id,
        product_id,
        list_of_promotion_line_level,
        rank_1
    FROM {{ source('raw', 'wi_so_pos_ll_promo_name_int1') }}
),

source_n_pricing_incentive_temp AS (
    SELECT
        bk_pricing_incentive_name,
        channel_prmtn_algnmt_cd,
        aligned_status_flg,
        pricing_incentive_group_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        pricing_incentive_type_cd
    FROM {{ source('raw', 'n_pricing_incentive_temp') }}
),

source_wi_erp_ar_bookings_init_2 AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        sales_territory_key,
        sales_rep_number,
        product_key,
        partner_site_party_key,
        adjustment_type_code,
        purchase_order_type_code,
        channel_bookings_flg,
        adjustment_code,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        bk_deal_id,
        dv_sales_order_type_alt_name,
        dv_source_order_num_int,
        dv_drct_val_added_dsti_ord_flg,
        bkgs_measure_trans_type_code,
        concession,
        standard_flag,
        net_price_usd_amt,
        list_price_usd_amt,
        standard_price_usd_amt,
        cost_price_usd_amt,
        extended_quantity,
        route_to_market_code,
        wips_originator_id_int
    FROM {{ source('raw', 'wi_erp_ar_bookings_init_2') }}
),

source_wi_promo_1 AS (
    SELECT
        bk_deal_id,
        product_id,
        list_of_promotion_line_level,
        ctr_flg
    FROM {{ source('raw', 'wi_promo_1') }}
),

source_wi_so_pos_ll_names AS (
    SELECT
        bk_deal_id,
        product_id,
        list_of_promotion_line_level,
        base_discount_name_line_level,
        non_standard_name_line_level,
        ctr_flg
    FROM {{ source('raw', 'wi_so_pos_ll_names') }}
),

source_wi_so_pos_discount_name AS (
    SELECT
        bk_deal_id,
        product_id,
        promotion_name,
        discount_name,
        source_reported_promotion_cd,
        discount_percent,
        discount_amount,
        active_yorn,
        hpefix_yorn
    FROM {{ source('raw', 'wi_so_pos_discount_name') }}
),

source_mt_2tier_incentive_bkgs_msr_temp AS (
    SELECT
        partner_site_party_key,
        sales_territory_key,
        product_key,
        sold_to_customer_key,
        sales_rep_num,
        wips_originator_id_int,
        sub_promotion_descr,
        dv_ar_transaction_dt,
        reported_end_user_name,
        dv_authorization_num,
        disti_promotion_num,
        promotion_category_type_cd,
        bk_pos_transaction_id_int,
        pos_transaction_dt,
        disti_to_reseller_so_num,
        dv_rslr_to_disti_po_num,
        bk_deal_id,
        distributor_reported_deal_id,
        pos_fiscal_year_month_int,
        dv_credit_memo_fsl_yr_mth_int,
        dv_l2net_category_type,
        dv_sub_concession_name,
        dv_deviation_status_cd,
        dv_stack_flg,
        dv_promo_split_pct,
        dv_pos_net_amt,
        dv_pos_list_amt,
        dv_pos_cost_amt,
        dv_adj_wpl_amt,
        dv_adj_extended_rebate_amt,
        dv_pos_with_claims_flg,
        dv_pos_without_claims_flg,
        dv_claims_without_pos_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        ide_adjustment_code,
        channel_prmtn_algnmt_cd,
        dv_disti_country_name,
        dv_pos_with_claims_pn_trx_flg,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        dv_route_to_market_cd
    FROM {{ source('raw', 'mt_2tier_incentive_bkgs_msr_temp') }}
),

final AS (
    SELECT
        bk_deal_id,
        product_id,
        promotion_name,
        discount_name,
        source_reported_promotion_cd,
        discount_percent,
        discount_amount,
        active_yorn,
        hpefix_yorn
    FROM source_mt_2tier_incentive_bkgs_msr_temp
)

SELECT * FROM final