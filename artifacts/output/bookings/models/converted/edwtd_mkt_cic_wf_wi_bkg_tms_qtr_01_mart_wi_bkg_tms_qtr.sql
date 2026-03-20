{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_bkg_tms_qtr', 'batch', 'edwtd_mkt_cic'],
    meta={
        'source_workflow': 'wf_m_WI_BKG_TMS_QTR',
        'target_table': 'WI_BKG_TMS_QTR',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.112999+00:00'
    }
) }}

WITH 

source_r_bkgs_by_quarter AS (
    SELECT
        fiscal_year_quarter_number_int,
        product_key,
        sales_territory_key,
        sales_rep_number,
        bill_to_customer_key,
        sold_to_customer_key,
        ship_to_customer_key,
        end_customer_key,
        bkgs_measure_trans_type_code,
        partner_party_site_key,
        wips_originator_id_int,
        salesrep_flag,
        service_flag,
        channel_booking_flag,
        extended_hold_quantity,
        extended_quantity,
        comp_us_list_price_amount,
        comp_us_hold_list_price_amt,
        comp_us_hold_net_price_amt,
        comp_us_hold_cost_amount,
        comp_us_net_price_amount,
        comp_us_cost_amount,
        comp_us_standard_price_amt,
        trade_in_amount,
        channel_drop_ship_flg,
        cancel_code
    FROM {{ source('raw', 'r_bkgs_by_quarter') }}
),

final AS (
    SELECT
        fiscal_year_quarter_number_int,
        advanced_technology_name,
        dd_customer_party_key,
        hq_customer_party_key,
        sales_territory_key,
        partner_site_party_key,
        comp_us_net_price_amount,
        comp_us_standard_price_amt,
        comp_us_list_price_amount,
        extended_quantity
    FROM source_r_bkgs_by_quarter
)

SELECT * FROM final