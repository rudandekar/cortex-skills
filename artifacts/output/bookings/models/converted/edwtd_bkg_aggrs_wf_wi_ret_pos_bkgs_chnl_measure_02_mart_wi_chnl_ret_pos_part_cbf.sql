{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_ret_pos_bkgs_chnl_measure', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_RET_POS_BKGS_CHNL_MEASURE',
        'target_table': 'WI_CHNL_RET_POS_PART_CBF',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.412364+00:00'
    }
) }}

WITH 

source_wi_chnl_ret_pos_part_country AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        partner_country,
        dv_end_cust_country
    FROM {{ source('raw', 'wi_chnl_ret_pos_part_country') }}
),

source_wi_chnl_ret_pos_dsf_pspk AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        partner_country,
        dv_end_cust_country,
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag
    FROM {{ source('raw', 'wi_chnl_ret_pos_dsf_pspk') }}
),

source_wi_chnl_ret_pos_part_cbf AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        partner_country,
        dv_end_cust_country,
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        level_1_sls_hierarchy,
        sales_channel_booking_flag
    FROM {{ source('raw', 'wi_chnl_ret_pos_part_cbf') }}
),

source_n_bookings_measure AS (
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
        conversion_dt,
        dd_bk_so_number_int,
        dd_cisco_booked_dtm,
        dd_sales_order_category_type,
        dd_sls_ord_operating_unit_cd,
        dd_trx_currency_cd,
        dv_transaction_type,
        adjustment_descr_key
    FROM {{ source('raw', 'n_bookings_measure') }}
),

source_n_channel_partner_site AS (
    SELECT
        partner_site_party_key,
        bk_party_id_int,
        partner_country_party_key,
        dd_grndparnt_partner_party_key,
        edw_create_user,
        edw_create_datetime,
        source_deleted_flg
    FROM {{ source('raw', 'n_channel_partner_site') }}
),

source_r_sales_hierarchy AS (
    SELECT
        sales_territory_key,
        l0_sales_territory_name_code,
        l1_sales_territory_name_code,
        l2_sales_territory_name_code,
        l3_sales_territory_name_code,
        l4_sales_territory_name_code,
        l5_sales_territory_name_code,
        l6_sales_territory_name_code,
        l7_sales_territory_name_code,
        sales_terr_effective_date,
        sales_terr_expiration_date,
        sales_coverage_code,
        sales_subcoverage_code,
        has_country_role,
        sales_territory_node_type,
        iso_country_code,
        sales_structure_ver_name,
        sales_structure_type,
        ss_erp_version_id_int,
        sales_territory_descr,
        l1_sales_territory_descr,
        l2_sales_territory_descr,
        l3_sales_territory_descr,
        l4_sales_territory_descr,
        l5_sales_territory_descr,
        l6_sales_territory_descr,
        l7_sales_territory_descr,
        bk_sales_territory_name,
        sales_territory_type_code,
        l1_sales_territory_sort_descr,
        l2_sales_territory_sort_descr,
        l3_sales_territory_sort_descr,
        l4_sales_territory_sort_descr,
        l5_sales_territory_sort_descr,
        l6_sales_territory_sort_descr,
        l7_sales_territory_sort_descr,
        dv_sales_terr_level_num_int
    FROM {{ source('raw', 'r_sales_hierarchy') }}
),

final AS (
    SELECT
        sold_to_customer_key,
        dv_end_customer_key,
        sales_territory_key,
        sold_to_party_key,
        dv_end_customer_party_key,
        partner_site_party_key_int,
        partner_party_key,
        partner_country,
        dv_end_cust_country,
        drop_ship_party_site_key,
        partner_site_party_key,
        channel_drop_ship_flag,
        level_1_sls_hierarchy,
        sales_channel_booking_flag,
        bk_wips_originator_id_int,
        ship_to_customer_key
    FROM source_r_sales_hierarchy
)

SELECT * FROM final