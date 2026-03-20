{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_gl_rev_msr_wwdisti_vld', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_GL_REV_MSR_WWDISTI_VLD',
        'target_table': 'WI_REV_MSR_ICPM_BKG_MIX',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.152098+00:00'
    }
) }}

WITH 

source_wi_bkg_msr_icpm_pos_alc_vld AS (
    SELECT
        l2_sales_territory_name_code,
        iso_country_code,
        sales_coverage_code,
        pos_sales_territory_key,
        sl6_comp_us_net_price_amt,
        scms_comp_us_net_price_amt,
        scms_pos_allocation_ratio
    FROM {{ source('raw', 'wi_bkg_msr_icpm_pos_alc_vld') }}
),

source_wi_gl_rev_msr_sales_hier AS (
    SELECT
        dv_fiscal_month_to_year_key,
        sales_territory_key,
        l1_sales_territory_name_code,
        l2_sales_territory_name_code,
        l3_sales_territory_name_code,
        sales_coverage_code,
        iso_country_code,
        sales_territory_type_code,
        l1_sales_territory_descr
    FROM {{ source('raw', 'wi_gl_rev_msr_sales_hier') }}
),

source_n_bkgs_measure AS (
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
        adjustment_descr_key,
        dv_transaction_name,
        bookings_split_pct,
        dv_source_order_num_int,
        dv_deal_id,
        dv_purchase_order_num,
        dv_booked_dt
    FROM {{ source('raw', 'n_bkgs_measure') }}
),

source_wi_gl_rev_msr_wwdisti AS (
    SELECT
        l3_sales_territory_name_code,
        product_key,
        sales_territory_key,
        iso_country_code,
        dv_fiscal_year_mth_number_int,
        dv_comp_us_net_price_amt,
        dv_comp_us_net_list_price_amt,
        dv_comp_us_gross_list_price_am,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_rev_amt,
        dv_comp_us_net_rev_amt,
        dv_comp_us_2tier_cmdm_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_standard_price_amt,
        dd_extended_net_qty,
        dd_extended_gross_qty,
        dv_credit_memo_amt,
        dv_debit_memo_amt,
        dv_inv_rev_base_list_amt,
        dv_shipped_rev_amt,
        dv_net_adj_amt,
        dv_rev_standard_cost_amt,
        dv_direct_rev_adj_amt,
        dv_direct_cost_adj_amt,
        dv_indirect_rev_adj_amt,
        dv_indirect_cogs_adj_amt,
        dv_gmb_cogs_adj_amt,
        dv_excess_obsolete_adj_amt,
        dv_overhead_adj_amt,
        dv_variance_adj_amt,
        dv_warranty_adj_amt
    FROM {{ source('raw', 'wi_gl_rev_msr_wwdisti') }}
),

source_mt_triangulation_type AS (
    SELECT
        triangulation_type_id_int,
        triangulation_type_desc,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_triangulation_type') }}
),

final AS (
    SELECT
        dv_fiscal_year_mth_number_int,
        l3_sales_territory_name_code,
        l2_sales_territory_name_code,
        iso_country_code,
        sales_coverage_code,
        scms_comp_us_net_price_amt,
        cty_comp_us_net_price_amt,
        scms_bkg_mix_ratio
    FROM source_mt_triangulation_type
)

SELECT * FROM final