{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_service_bookings_by_pid_psg_be', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SERVICE_BOOKINGS_BY_PID_PSG_BE',
        'target_table': 'WI_PSG_BE_PID_Non_CMS_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.438026+00:00'
    }
) }}

WITH 

source_wi_psg_be_pid_non_cms_map AS (
    SELECT
        service_prod_subgroup_id,
        bk_service_category_id,
        bk_product_id,
        psg_be_alloc_pct
    FROM {{ source('raw', 'wi_psg_be_pid_non_cms_map') }}
),

source_wi_psg_be_pid_cms_map AS (
    SELECT
        service_prod_subgroup_id,
        bk_service_category_id,
        bk_product_id,
        psg_be_alloc_pct
    FROM {{ source('raw', 'wi_psg_be_pid_cms_map') }}
),

source_wi_service_bookings_by_pid AS (
    SELECT
        bookings_measure_key,
        sales_order_key,
        sales_order_line_key,
        product_key,
        ar_trx_line_key,
        ar_trx_key,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_end_customer_key,
        partner_site_party_key,
        adjustment_descr_key,
        revenue_transfer_key,
        transaction_datetime,
        sales_territory_key,
        l1_sales_territory_descr,
        sales_rep_number,
        bookings_process_date,
        dv_fiscal_year_mth_number_int,
        fiscal_year_quarter_number_int,
        bk_pos_transaction_id_int,
        bk_sales_adj_line_number_int,
        bk_sales_adj_number_int,
        adjustment_type_code,
        bkgs_measure_trans_type_code,
        service_flg,
        dv_revenue_recognition_flg,
        corporate_bookings_flg,
        trade_in_amount,
        dd_comp_us_net_price_amount,
        tss_bookings_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        dd_comp_us_hold_net_price_amt,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amount,
        dd_extended_hold_quantity,
        dd_comp_us_standard_price_amt,
        wips_originator_id_int,
        adjustment_code,
        dv_transaction_name,
        serviceordernumber,
        dd_cisco_booked_dtm,
        dv_attribution_cd,
        dv_product_key,
        dv_sales_order_line_key,
        dv_ar_trx_line_key,
        xaas_offer_atrbtn_rev_line_key,
        rstd_del_record_flg,
        dv_net_spread_flg,
        service_sku,
        product_family_id,
        service_prod_subgroup_id,
        goods_product_key,
        goods_product_id,
        quote_number,
        order_alloc_pct,
        quote_alloc_pct,
        pid_pct,
        record_type,
        sales_motion_cd,
        summary_quote_flg,
        sk_sales_motion_attrib_key,
        dv_annualized_us_net_amt,
        dv_multiyear_us_net_amt,
        transaction_ai_flg,
        ai_intent_type_cd,
        trx_ai_product_class_name,
        frst_yr_annual_usd_amt,
        multi_year_usd_amt,
        non_recur_one_tm_cr_usd_amt,
        first_year_credit_usd_amt,
        multi_year_credit_usd_amt
    FROM {{ source('raw', 'wi_service_bookings_by_pid') }}
),

final AS (
    SELECT
        service_prod_subgroup_id,
        bk_service_category_id,
        bk_product_id,
        psg_be_alloc_pct
    FROM source_wi_service_bookings_by_pid
)

SELECT * FROM final