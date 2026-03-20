{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_bookings_channel_measure_retro', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_N_BOOKINGS_CHANNEL_MEASURE_RETRO',
        'target_table': 'GCSP_ATTRIB_POS_RETRO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.882383+00:00'
    }
) }}

WITH 

source_gcsp_attrib_sls_adj_retro AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        bill_to_customer_key,
        sales_adjustment_date,
        partner_site_party_key,
        be_id,
        be_geo_id,
        end_customer_gu_id,
        end_customer_gu_name,
        partner_country,
        end_customer_country,
        partner_bill_to_id
    FROM {{ source('raw', 'gcsp_attrib_sls_adj_retro') }}
),

source_gcsp_attrib_pos_retro AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        sold_to_customer_key,
        pos_order_dt,
        partner_site_party_key,
        be_id,
        be_geo_id,
        end_customer_gu_id,
        end_customer_gu_name,
        partner_country,
        end_customer_country,
        partner_bill_to_id
    FROM {{ source('raw', 'gcsp_attrib_pos_retro') }}
),

source_gcsp_attrib_ar_retro AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        bill_to_customer_key,
        bk_so_src_crt_datetime,
        partner_site_party_key,
        be_id,
        be_geo_id,
        end_customer_gu_id,
        end_customer_gu_name,
        partner_country,
        end_customer_country,
        partner_bill_to_id
    FROM {{ source('raw', 'gcsp_attrib_ar_retro') }}
),

source_gcsp_attrib_xaas_retro AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        bill_to_customer_key,
        booked_dt,
        partner_site_party_key,
        be_id,
        be_geo_id,
        end_customer_gu_id,
        end_customer_gu_name,
        partner_country,
        end_customer_country,
        partner_bill_to_id
    FROM {{ source('raw', 'gcsp_attrib_xaas_retro') }}
),

source_gcsp_attrib_erp_retro AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        bill_to_customer_key,
        bk_so_src_crt_datetime,
        partner_site_party_key,
        be_id,
        be_geo_id,
        end_customer_gu_id,
        end_customer_gu_name,
        partner_country,
        end_customer_country,
        partner_bill_to_id
    FROM {{ source('raw', 'gcsp_attrib_erp_retro') }}
),

source_wi_ret_bkgs_chnl_measure AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        partner_site_party_key,
        channel_bookings_flg,
        channel_drop_ship_flg,
        as_of_fsc_mth_ptr_ste_prty_key,
        as_of_fsc_mth_channel_bkgs_flg,
        as_of_fsc_mth_chnl_drp_shp_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        dv_route_to_market_description,
        otm_override_ptnr_site_party_key,
        mnl_ptnr_site_override_flg,
        manual_ptnr_override_reason_cd
    FROM {{ source('raw', 'wi_ret_bkgs_chnl_measure') }}
),

source_w_bookings_measure AS (
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
        transaction_datetime,
        sales_territory_key,
        sales_rep_number,
        bookings_process_date,
        dv_fiscal_year_mth_number_int,
        bk_pos_transaction_id_int,
        bk_sales_adj_line_number_int,
        bk_sales_adj_number_int,
        adjustment_type_code,
        sales_channel_code,
        sales_credit_type_code,
        ide_adjustment_code,
        adjustment_code,
        bkgs_measure_trans_type_code,
        cancelled_flg,
        cancel_code,
        acquisition_flg,
        forward_reverse_flg,
        distributor_offset_flg,
        corporate_bookings_flg,
        overlay_flg,
        ic_revenue_flg,
        charges_flg,
        salesrep_flg,
        misc_flg,
        service_flg,
        international_demo_flg,
        replacement_demo_flg,
        revenue_flg,
        rma_flg,
        trade_in_amount,
        dd_comp_us_net_price_amount,
        dd_comp_us_list_price_amount,
        dd_comp_us_cost_amount,
        dd_extended_quantity,
        dd_comp_us_hold_net_price_amt,
        dd_comp_us_hold_list_price_amt,
        dd_comp_us_hold_cost_amount,
        dd_extended_hold_quantity,
        dd_comp_us_standard_price_amt,
        wips_originator_id_int,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime,
        conversion_rt,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_bookings_measure') }}
),

final AS (
    SELECT
        bookings_measure_key,
        bookings_process_date,
        bkgs_measure_trans_type_code,
        sold_to_customer_key,
        pos_order_dt,
        partner_site_party_key,
        be_id,
        be_geo_id,
        end_customer_gu_id,
        end_customer_gu_name,
        partner_country,
        end_customer_country,
        partner_bill_to_id
    FROM source_w_bookings_measure
)

SELECT * FROM final