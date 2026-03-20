{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_wi_drvd_ncr_bkg_trx_recomp_int', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_DRVD_NCR_BKG_TRX_RECOMP_INT',
        'target_table': 'WI_PROCESS_DATES_DRVD_RECOMP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.724534+00:00'
    }
) }}

WITH 

source_n_day AS (
    SELECT
        bk_calendar_date,
        bk_fiscal_year_number_int,
        bk_fiscal_week_number_int,
        bk_calendar_week_start_date,
        bk_fiscal_calendar_code,
        dv_clndr_ytd_flag,
        dv_clndr_qtd_flag,
        dv_clndr_mtd_flag,
        dv_clndr_wtd_flag,
        dv_fiscal_ytd_flag,
        dv_fiscal_qtd_flag,
        dv_fiscal_mtd_flag,
        dv_fiscal_wtd_flag,
        bk_calendar_month_number_int,
        bk_calendar_year_int,
        edw_create_user,
        edw_create_datetime,
        edw_update_user,
        edw_update_datetime
    FROM {{ source('raw', 'n_day') }}
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
        edw_update_datetime
    FROM {{ source('raw', 'n_drvd_ncr_bkg_trx') }}
),

final AS (
    SELECT
        bk_calendar_date
    FROM source_n_drvd_ncr_bkg_trx
)

SELECT * FROM final