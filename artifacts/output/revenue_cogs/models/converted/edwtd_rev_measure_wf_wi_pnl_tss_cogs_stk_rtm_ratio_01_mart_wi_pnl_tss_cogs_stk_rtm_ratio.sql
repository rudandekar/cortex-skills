{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_tss_cogs_stk_rtm_ratio', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_TSS_COGS_STK_RTM_RATIO',
        'target_table': 'WI_PNL_TSS_COGS_STK_RTM_RATIO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.655412+00:00'
    }
) }}

WITH 

source_wi_pnl_tss_cogs_stk_rtm_ratio AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        dv_pnl_rev_cost_type,
        bk_revenue_or_cogs_type_cd,
        original_sales_territory_key,
        channel_flg,
        as_of_fsc_mth_channel_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        tss_cogs_amt,
        stk_rtm_ratio
    FROM {{ source('raw', 'wi_pnl_tss_cogs_stk_rtm_ratio') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        dv_pnl_rev_cost_type,
        bk_revenue_or_cogs_type_cd,
        original_sales_territory_key,
        channel_flg,
        as_of_fsc_mth_channel_flg,
        dv_route_to_market_cd,
        dv_as_of_fsc_mth_rtm_cd,
        tss_cogs_amt,
        stk_rtm_ratio
    FROM source_wi_pnl_tss_cogs_stk_rtm_ratio
)

SELECT * FROM final