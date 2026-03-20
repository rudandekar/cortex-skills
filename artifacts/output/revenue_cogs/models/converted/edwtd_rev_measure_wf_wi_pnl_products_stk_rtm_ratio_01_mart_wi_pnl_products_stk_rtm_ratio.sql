{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_products_stk_rtm_ratio', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_PRODUCTS_STK_RTM_RATIO',
        'target_table': 'WI_PNL_PRODUCTS_STK_RTM_RATIO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.976301+00:00'
    }
) }}

WITH 

source_wi_pnl_products_stk_rtm_ratio AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_revenue_or_cogs_type_cd,
        bk_sales_territory_key,
        dv_pnl_line_item_name,
        partner_type_cd,
        channel_flg,
        as_of_fsc_mth_partner_type_cd,
        as_of_fsc_mth_channel_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        comp_us_net_rev_amt,
        stk_rtm_ratio
    FROM {{ source('raw', 'wi_pnl_products_stk_rtm_ratio') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_revenue_or_cogs_type_cd,
        bk_sales_territory_key,
        dv_pnl_line_item_name,
        partner_type_cd,
        channel_flg,
        as_of_fsc_mth_partner_type_cd,
        as_of_fsc_mth_channel_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        comp_us_net_rev_amt,
        stk_rtm_ratio
    FROM source_wi_pnl_products_stk_rtm_ratio
)

SELECT * FROM final