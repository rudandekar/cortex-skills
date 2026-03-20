{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_wi_drvd_nrt_bkg_trx_recomp_int', 'batch', 'edwtd_ncrnrt_bkg'],
    meta={
        'source_workflow': 'wf_m_WI_DRVD_NRT_BKG_TRX_RECOMP_INT',
        'target_table': 'WI_PROCESS_DATES_DRVD_RECOMP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.373820+00:00'
    }
) }}

WITH 

source_mt_drvd_nrt_bkg_trx AS (
    SELECT
        bookings_measure_key,
        bk_fiscal_month_number_int,
        dd_extended_hold_quantity,
        conversion_dt,
        sales_rep_number,
        transaction_datetime,
        transaction_sequence_id_int,
        sales_channel_code,
        cdb_data_source_code,
        split_percentage,
        bk_fiscal_year_number_int,
        dd_service_flag,
        dd_corp_bkg_flag,
        dd_comp_us_net_price_amount,
        dd_comp_us_hold_net_price_amt,
        sold_to_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        conversion_rt,
        sales_order_line_key,
        bk_so_number_int,
        purchase_order_number,
        bk_so_src_crt_datetime,
        forward_reverse_code,
        bookings_percentage,
        net_price_amount,
        transaction_quantity,
        process_date,
        bk_iso_currency_code,
        sales_order_key,
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
        corp_bkg_recompute_flag,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amount,
        es_line_seq_id_int,
        order_dtm,
        cancelled_flg,
        ru_cisco_booked_datetime,
        sales_order_category_type,
        source_system_code,
        dd_comp_us_standard_price_amt,
        dv_fiscal_year_mth_number_int,
        sales_order_operating_unit,
        product_key,
        sales_territory_key,
        sales_credit_type_code,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_attribution_cd,
        sk_offer_attribution_id_int,
        dv_sales_order_line_key,
        dv_product_key
    FROM {{ source('raw', 'mt_drvd_nrt_bkg_trx') }}
),

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

final AS (
    SELECT
        bk_calendar_date
    FROM source_n_day
)

SELECT * FROM final