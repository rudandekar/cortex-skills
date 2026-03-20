{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_direct_data_ar_napld_incr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_WI_DIRECT_DATA_AR_NAPLD_INCR',
        'target_table': 'WI_DIRECT_DATA_AR_NAPLD_INCR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.106024+00:00'
    }
) }}

WITH 

source_wi_direct_data_ar_napld_incr AS (
    SELECT
        bookings_measure_key,
        sales_order_key,
        sales_order_line_key,
        product_key,
        ar_trx_line_key,
        ar_trx_key,
        sales_territory_key,
        sales_rep_number,
        bookings_process_date,
        dv_fiscal_year_mth_number_int,
        bk_pos_transaction_id_int,
        bk_sales_adj_line_number_int,
        bk_sales_adj_number_int,
        bkgs_measure_trans_type_code,
        forward_reverse_flg,
        distributor_offset_flg,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        dd_comp_us_standard_price_amt,
        edw_create_datetime,
        edw_update_datetime,
        bookings_split_pct,
        dv_sales_order_line_key,
        so_sbscrptn_itm_sls_trx_key,
        service_flg,
        process_date_rk,
        source_rep_annual_usd_amt,
        dv_annualized_us_net_amt,
        dv_multiyear_us_net_amt,
        sk_offer_attribution_id_int,
        sk_sales_motion_attrib_key,
        line_creation_date,
        revenue_transfer_key
    FROM {{ source('raw', 'wi_direct_data_ar_napld_incr') }}
),

final AS (
    SELECT
        bookings_measure_key,
        sales_order_key,
        sales_order_line_key,
        product_key,
        ar_trx_line_key,
        ar_trx_key,
        sales_territory_key,
        sales_rep_number,
        bookings_process_date,
        dv_fiscal_year_mth_number_int,
        bk_pos_transaction_id_int,
        bk_sales_adj_line_number_int,
        bk_sales_adj_number_int,
        bkgs_measure_trans_type_code,
        forward_reverse_flg,
        distributor_offset_flg,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        dd_comp_us_standard_price_amt,
        edw_create_datetime,
        edw_update_datetime,
        bookings_split_pct,
        dv_sales_order_line_key,
        so_sbscrptn_itm_sls_trx_key,
        service_flg,
        process_date_rk,
        dv_ar_trx_line_key,
        xaas_offer_atrbtn_rev_line_key,
        source_rep_annual_usd_amt,
        dv_annualized_us_net_amt,
        dv_multiyear_us_net_amt,
        sk_offer_attribution_id_int,
        sk_sales_motion_attrib_key,
        line_creation_date,
        revenue_transfer_key
    FROM source_wi_direct_data_ar_napld_incr
)

SELECT * FROM final