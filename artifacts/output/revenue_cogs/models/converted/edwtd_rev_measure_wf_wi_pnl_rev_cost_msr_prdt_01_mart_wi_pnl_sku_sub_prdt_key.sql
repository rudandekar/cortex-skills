{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_rev_cost_msr_prdt', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_REV_COST_MSR_PRDT',
        'target_table': 'WI_PNL_SKU_SUB_PRDT_KEY',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.589446+00:00'
    }
) }}

WITH 

source_wi_pnl_sku_sub_prdt_key AS (
    SELECT
        product_key
    FROM {{ source('raw', 'wi_pnl_sku_sub_prdt_key') }}
),

source_wi_pnl_prdt_company_cd AS (
    SELECT
        fiscal_year_month_int,
        bk_company_code,
        iso_country_name
    FROM {{ source('raw', 'wi_pnl_prdt_company_cd') }}
),

source_wi_pnl_rev_cost_msr_prdt AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_revenue_or_cogs_type_cd,
        bk_product_key,
        bk_financial_account_cd,
        bk_rev_measure_trnsctn_type_cd,
        bk_sales_territory_key,
        sales_order_line_key,
        comp_us_net_rev_amt,
        bk_company_cd,
        bk_iso_country_cd,
        dv_pnl_line_item_name,
        ce_country_name,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        partner_type_cd,
        channel_flg,
        channel_drop_ship_flg,
        as_of_fsc_mth_partner_type_cd,
        as_of_fsc_mth_channel_flg,
        as_of_fsc_mth_chnl_drp_shp_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        deal_ss_cd,
        dv_deal_id,
        xcat_flg,
        bk_offer_type_name,
        recurring_offer_flg,
        ela_flg,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr_prdt') }}
),

final AS (
    SELECT
        product_key
    FROM source_wi_pnl_rev_cost_msr_prdt
)

SELECT * FROM final