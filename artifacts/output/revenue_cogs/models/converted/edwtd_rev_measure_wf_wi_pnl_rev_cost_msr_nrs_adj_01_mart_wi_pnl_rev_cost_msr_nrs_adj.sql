{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_rev_cost_msr_nrs_adj', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_REV_COST_MSR_NRS_ADJ',
        'target_table': 'WI_PNL_REV_COST_MSR_NRS_ADJ',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.198131+00:00'
    }
) }}

WITH 

source_wi_pnl_rev_cost_msr_nrs_adj AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_revenue_or_cogs_type_cd,
        bk_product_key,
        bk_sales_territory_key,
        comp_us_net_rev_amt,
        dv_pnl_line_item_name,
        bk_offer_type_name
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr_nrs_adj') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_revenue_or_cogs_type_cd,
        bk_product_key,
        bk_sales_territory_key,
        comp_us_net_rev_amt,
        dv_pnl_line_item_name,
        bk_offer_type_name
    FROM source_wi_pnl_rev_cost_msr_nrs_adj
)

SELECT * FROM final