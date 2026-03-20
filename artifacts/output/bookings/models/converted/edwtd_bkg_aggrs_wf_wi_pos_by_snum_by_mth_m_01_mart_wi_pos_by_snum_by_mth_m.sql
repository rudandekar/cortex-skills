{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pos_by_snum_by_mth_m', 'batch', 'edwtd_bkg_aggrs'],
    meta={
        'source_workflow': 'wf_m_WI_POS_BY_SNUM_BY_MTH_M',
        'target_table': 'WI_POS_BY_SNUM_BY_MTH_M',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.437848+00:00'
    }
) }}

WITH 

source_wi_pos_by_snum_by_mth_m AS (
    SELECT
        product_net_price_amount,
        pos_trx_line_product_quant,
        vldtd_net_unit_price_usd_amt,
        valuation_price_usd_amount,
        pos_product_key,
        bk_ship_to_wips_site_use_key,
        bk_bill_to_wips_site_use_key,
        bk_sold_to_wips_site_use_key,
        bk_end_user_wips_site_use_key,
        sold_to_sales_territory_key,
        bk_wips_originator_id_int,
        dv_fiscal_year_mth_number_int,
        partner_site_party_key,
        pos_trx_line_posted_date,
        second_source_flag,
        scrubbed_serial_number,
        pos_trx_line_active_flag,
        pos_source_type,
        base_list_unit_prod_price_amt,
        rptd_unit_price_usd_amount,
        bk_pos_transaction_id_int,
        distributor_reported_deal_id,
        bk_deal_id,
        transaction_date,
        disti_to_reseller_so_number,
        disti_2_cisco_po_number,
        ru_rslr_to_disti_po_number,
        end_cust_to_reseller_po_num,
        market_type_code,
        dist_rptd_sales_rep_name
    FROM {{ source('raw', 'wi_pos_by_snum_by_mth_m') }}
),

final AS (
    SELECT
        product_net_price_amount,
        pos_trx_line_product_quant,
        vldtd_net_unit_price_usd_amt,
        valuation_price_usd_amount,
        pos_product_key,
        bk_ship_to_wips_site_use_key,
        bk_bill_to_wips_site_use_key,
        bk_sold_to_wips_site_use_key,
        bk_end_user_wips_site_use_key,
        sold_to_sales_territory_key,
        bk_wips_originator_id_int,
        dv_fiscal_year_mth_number_int,
        partner_site_party_key,
        pos_trx_line_posted_date,
        second_source_flag,
        scrubbed_serial_number,
        pos_trx_line_active_flag,
        pos_source_type,
        base_list_unit_prod_price_amt,
        rptd_unit_price_usd_amount,
        bk_pos_transaction_id_int,
        distributor_reported_deal_id,
        bk_deal_id,
        transaction_date,
        disti_to_reseller_so_number,
        disti_2_cisco_po_number,
        ru_rslr_to_disti_po_number,
        end_cust_to_reseller_po_num,
        market_type_code,
        dist_rptd_sales_rep_name
    FROM source_wi_pos_by_snum_by_mth_m
)

SELECT * FROM final