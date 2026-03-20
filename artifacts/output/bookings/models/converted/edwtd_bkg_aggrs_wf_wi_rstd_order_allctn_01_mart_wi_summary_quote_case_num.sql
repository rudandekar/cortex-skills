{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_rstd_order_allctn', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_RSTD_ORDER_ALLCTN',
        'target_table': 'WI_SUMMARY_QUOTE_CASE_NUM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.766657+00:00'
    }
) }}

WITH 

source_wi_summary_quote_case_num AS (
    SELECT
        bk_quote_num,
        summary_quote_case_num,
        bk_so_number_int,
        fiscal_year_month_int,
        fiscal_quarter_name,
        fiscal_year_number_int,
        service_quote_net_trxl_amt,
        dv_service_quote_net_usd_amt
    FROM {{ source('raw', 'wi_summary_quote_case_num') }}
),

source_wi_quote_order AS (
    SELECT
        fiscal_month,
        quote_number,
        contract_quote_number,
        order_number
    FROM {{ source('raw', 'wi_quote_order') }}
),

source_el_tech_line_edw_sum_quote AS (
    SELECT
        bk_service_contract_num,
        bk_quote_num,
        maintenance_order_num,
        bk_so_number_int,
        summary_quote_case_num,
        bk_product_id,
        ip_key,
        service_type,
        instance_num,
        du_bk_serial_num,
        dv_net_prc_ttl_usd_amt,
        dv_lst_prc_ttl_usd_amt,
        fiscal_quarter_name,
        fiscal_year_number_int,
        fiscal_year_month_int
    FROM {{ source('raw', 'el_tech_line_edw_sum_quote') }}
),

source_el_quote_snapshot AS (
    SELECT
        quote_number,
        order_number,
        quote_status_cd,
        contract_num,
        target_contract_num,
        coverage_start_dt,
        coverage_end_dt,
        service_type,
        cle_id_renewed,
        sk_instance_id_int,
        product_id,
        serial_number,
        shipped_date,
        quote_line_status_cd,
        total_list_price,
        maintenance_list_price,
        list_quote_price,
        order_quote_key,
        svcsku_key,
        system_source_quotes,
        fiscal_year,
        quarter_ref,
        fiscal_period_id
    FROM {{ source('raw', 'el_quote_snapshot') }}
),

source_wi_rstd_order_allctn AS (
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
        order_net_price,
        order_psg_total_price,
        order_alloc_pct,
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
        finance_productsubgroup,
        service_pid,
        quote_number,
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
    FROM {{ source('raw', 'wi_rstd_order_allctn') }}
),

final AS (
    SELECT
        bk_quote_num,
        summary_quote_case_num,
        bk_so_number_int,
        fiscal_year_month_int,
        fiscal_quarter_name,
        fiscal_year_number_int,
        service_quote_net_trxl_amt,
        dv_service_quote_net_usd_amt
    FROM source_wi_rstd_order_allctn
)

SELECT * FROM final