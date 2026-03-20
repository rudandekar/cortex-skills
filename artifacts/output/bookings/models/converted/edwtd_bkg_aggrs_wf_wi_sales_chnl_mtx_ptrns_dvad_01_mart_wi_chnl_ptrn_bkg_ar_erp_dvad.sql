{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_sales_chnl_mtx_ptrns_dvad', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_SALES_CHNL_MTX_PTRNS_DVAD',
        'target_table': 'WI_CHNL_PTRN_BKG_AR_ERP_DVAD',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.246842+00:00'
    }
) }}

WITH 

source_wi_chnl_ptrn_bkg_ar_erp_dvad AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        end_customer_type_code,
        process_date,
        sales_order_key,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM {{ source('raw', 'wi_chnl_ptrn_bkg_ar_erp_dvad') }}
),

source_n_drvd_ncr_bkg_trx AS (
    SELECT
        drvd_ncr_bkg_trx_key,
        forward_reverse_code,
        transaction_datetime,
        transaction_sequence_id_int,
        source_system_code,
        process_date,
        bookings_percentage,
        net_price_amount,
        sales_channel_code,
        split_percentage,
        trade_in_amount,
        transaction_grouping_type_code,
        transaction_quantity,
        cdb_data_source_code,
        sales_rep_number,
        sales_credit_type_code,
        dd_item_type_code_flag,
        dd_rma_flag,
        dd_international_demo_flag,
        dd_replacement_demo_flag,
        dd_revenue_flag,
        dd_overlay_flag,
        dd_salesrep_flag,
        dd_ic_revenue_flag,
        dd_charges_flag,
        dd_misc_flag,
        dd_acquisition_flag,
        dd_service_flag,
        dd_corp_bkg_flag,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        dd_comp_us_hold_net_price_amt,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amount,
        dd_extended_hold_quantity,
        sales_territory_key,
        bk_iso_currency_code,
        corp_bkg_recompute_flag,
        dd_comp_us_standard_price_amt,
        bk_fiscal_calendar_code,
        bk_fiscal_year_number_int,
        bk_fiscal_month_number_int,
        dv_fiscal_year_mth_number_int,
        sales_order_key,
        ship_to_customer_key,
        bill_to_customer_key,
        sold_to_customer_key,
        product_key,
        sales_order_line_key,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        conversion_rt,
        order_dtm
    FROM {{ source('raw', 'n_drvd_ncr_bkg_trx') }}
),

final AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        end_customer_type_code,
        process_date,
        sales_order_key,
        edw_create_user,
        edw_create_datetime,
        ship_to_customer_key
    FROM source_n_drvd_ncr_bkg_trx
)

SELECT * FROM final