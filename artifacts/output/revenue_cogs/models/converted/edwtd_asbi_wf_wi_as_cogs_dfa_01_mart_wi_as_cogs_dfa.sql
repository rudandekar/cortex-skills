{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_cogs_dfa', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_AS_COGS_DFA',
        'target_table': 'WI_AS_COGS_DFA',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.219723+00:00'
    }
) }}

WITH 

source_wi_as_cogs_dfa AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_revenue_or_cogs_type_cd,
        bk_product_key,
        goods_product_key,
        bk_sales_territory_key,
        bk_trngltd_sales_territory_key,
        comp_us_net_rev_amt1,
        dv_product_type,
        dv_pnl_line_item_name,
        dv_recurring_offer_cd,
        bk_as_project_cd
    FROM {{ source('raw', 'wi_as_cogs_dfa') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_revenue_or_cogs_type_cd,
        bk_product_key,
        goods_product_key,
        bk_sales_territory_key,
        bk_trngltd_sales_territory_key,
        comp_us_net_rev_amt1,
        dv_product_type,
        dv_pnl_line_item_name,
        dv_recurring_offer_cd,
        bk_as_project_cd
    FROM source_wi_as_cogs_dfa
)

SELECT * FROM final