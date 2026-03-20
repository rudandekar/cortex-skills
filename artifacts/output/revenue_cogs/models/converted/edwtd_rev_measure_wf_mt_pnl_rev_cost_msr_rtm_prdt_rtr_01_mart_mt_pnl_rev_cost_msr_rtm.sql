{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_pnl_rev_cost_msr_rtm_prdt_rtr', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_PNL_REV_COST_MSR_RTM_PRDT_RTR',
        'target_table': 'MT_PNL_REV_COST_MSR_RTM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.655148+00:00'
    }
) }}

WITH 

source_mt_pnl_rev_cost_msr_rtm AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        dv_pnl_rev_cost_type,
        bk_revenue_or_cogs_type_cd,
        original_sales_territory_key,
        dv_pnl_line_item_name,
        partner_type_cd,
        channel_flg,
        as_of_fsc_mth_partner_type_cd,
        as_of_fsc_mth_channel_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        stk_rtm_ratio,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'mt_pnl_rev_cost_msr_rtm') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        dv_pnl_rev_cost_type,
        bk_revenue_or_cogs_type_cd,
        original_sales_territory_key,
        dv_pnl_line_item_name,
        partner_type_cd,
        channel_flg,
        as_of_fsc_mth_partner_type_cd,
        as_of_fsc_mth_channel_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        stk_rtm_ratio,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_mt_pnl_rev_cost_msr_rtm
)

SELECT * FROM final