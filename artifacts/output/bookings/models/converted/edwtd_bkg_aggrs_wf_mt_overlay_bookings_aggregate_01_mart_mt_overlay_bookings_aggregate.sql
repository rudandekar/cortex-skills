{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_overlay_bookings_aggregate', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_MT_OVERLAY_BOOKINGS_AGGREGATE',
        'target_table': 'MT_OVERLAY_BOOKINGS_AGGREGATE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.453637+00:00'
    }
) }}

WITH 

source_mt_overlay_bookings_measure AS (
    SELECT
        overlay_bookings_measure_key,
        overlay_bookings_process_date,
        ovrly_bkgs_msr_trans_type_code,
        sales_order_key,
        dd_bk_so_number_int,
        sales_order_line_key,
        ar_trx_line_key,
        bk_pos_transaction_id_int,
        manual_trx_key,
        overlay_sales_territory_key,
        direct_sales_territory_key,
        overlay_sales_rep_number,
        direct_sales_rep_number,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        end_customer_key,
        product_key,
        bookings_process_date,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        dv_fiscal_year_mth_number_int,
        bk_sls_terr_assignment_type_cd,
        overlay_sales_commission_pct,
        overlay_sales_creation_dt,
        service_flg,
        forward_reverse_flg,
        latest_record_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_attribution_cd,
        dv_product_key,
        dv_sales_order_line_key,
        sk_offer_attribution_id_int,
        dv_fmv_flg,
        corporate_bookings_flg,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        dsv_flg,
        rd_end_customer_key,
        rd_bill_to_customer_key,
        rd_ship_to_customer_key,
        rd_sold_to_customer_key,
        rd_overlay_sales_territory_key
    FROM {{ source('raw', 'mt_overlay_bookings_measure') }}
),

final AS (
    SELECT
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        overlay_sales_territory_key,
        overlay_sales_rep_num,
        service_flg,
        ovrly_bkgs_msr_trans_type_cd,
        sls_terr_assignment_type_cd,
        book_net_wtd_amt,
        book_net_mtd_amt,
        book_net_qtd_amt,
        book_net_ytd_amt,
        edw_create_dtm,
        edw_create_user,
        dv_attribution_cd,
        dv_fmv_flg,
        corporate_bookings_flg,
        dv_revenue_recognition_flg,
        dv_net_spread_flg,
        dsv_flg,
        rd_end_customer_key,
        rd_bill_to_customer_key,
        rd_ship_to_customer_key,
        rd_sold_to_customer_key,
        rd_overlay_sales_territory_key
    FROM source_mt_overlay_bookings_measure
)

SELECT * FROM final