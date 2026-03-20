{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sbp_gross_unbilled_rvenue', 'batch', 'edwtd_ar'],
    meta={
        'source_workflow': 'wf_m_WI_SBP_GROSS_UNBILLED_RVENUE',
        'target_table': 'WI_SUBSCR_INVOICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.160311+00:00'
    }
) }}

WITH 

source_wi_sbp_gross_unbilled_revenue AS (
    SELECT
        transaction_type,
        rpo_flg,
        unbilled_revenue_amt,
        processed_fiscal_mth,
        fiscal_mth,
        src_entity,
        sales_territory_key,
        product_key,
        sales_order_key,
        sales_order_line_key,
        web_order_id,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_cust_acct_key,
        end_customer_key,
        service_flg,
        corp_flg,
        contract_start_dtm,
        contract_end_dtm,
        contract_term,
        contract_num,
        erp_deal_id,
        auto_release_dt,
        fhi_bni_flg,
        cisco_booked_dt,
        oracle_book_dtm,
        process_dt,
        scheduled_ship_dt,
        hold_flg,
        early_ship_flg,
        shipped_not_invoiced_flg,
        cust_rqstd_shipment_dt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        attributed_product_key,
        dv_attribution_cd,
        dv_recurring_offer_cd,
        dv_total_sales_value_amt,
        dd_fb_trxl_currency_cd,
        pl_conversion_rate,
        dd_fb_so_line_src_create_dtm,
        dd_fb_promised_to_shipment_dt,
        dd_fb_ss_cd,
        subscription_line_tsv_key,
        subscription_ref_id,
        bk_exaas_subscr_num,
        subscription_status_cd,
        initial_term_mths_cnt,
        renewal_term_mths_cnt,
        prepaid_term_mths_cnt,
        renewal_cnt,
        billed_day_of_the_mth_int,
        offer_type_cd,
        billing_model_name,
        subscr_line_status_cd,
        sales_order_type_cd,
        attribution_pct,
        src_cancel_dtm,
        charge_type_cd,
        ato_name,
        dv_order_quantity,
        dv_comp_us_list_price_amount,
        dv_sales_commission_pct,
        flex_up_resv_line_flg,
        order_origin_cd,
        dv_rpo_active_flg
    FROM {{ source('raw', 'wi_sbp_gross_unbilled_revenue') }}
),

source_wi_subscr_invoice AS (
    SELECT
        subscription_line_tsv_key,
        subscription_ref_id,
        bk_exaas_subscr_num,
        product_key,
        invoice_amount
    FROM {{ source('raw', 'wi_subscr_invoice') }}
),

final AS (
    SELECT
        subscription_line_tsv_key,
        subscription_ref_id,
        bk_exaas_subscr_num,
        product_key,
        invoice_amount
    FROM source_wi_subscr_invoice
)

SELECT * FROM final