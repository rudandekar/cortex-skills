{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_rev_cost_msr_srvc_rev', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_REV_COST_MSR_SRVC_REV',
        'target_table': 'WI_PNL_REV_COST_MSR_SRVC_REV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.483526+00:00'
    }
) }}

WITH 

source_wi_pnl_rev_cost_msr_srvc_rev AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_product_key,
        goods_product_key,
        bk_financial_account_cd,
        bk_rev_measure_trnsctn_type_cd,
        dv_triangulation_type_id_int,
        bk_prdt_subgroup_alloc_src_cd,
        bk_sales_territory_key,
        original_sales_territory_key,
        bk_trngltd_sales_territory_key,
        sales_order_line_key,
        comp_us_net_rev_amt,
        dv_pnl_line_item_name,
        dv_pnl_rev_cost_type,
        deal_ss_cd,
        dv_deal_id,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr_srvc_rev') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_product_key,
        goods_product_key,
        bk_financial_account_cd,
        bk_rev_measure_trnsctn_type_cd,
        dv_triangulation_type_id_int,
        bk_prdt_subgroup_alloc_src_cd,
        bk_sales_territory_key,
        original_sales_territory_key,
        bk_trngltd_sales_territory_key,
        sales_order_line_key,
        comp_us_net_rev_amt,
        dv_pnl_line_item_name,
        dv_pnl_rev_cost_type,
        deal_ss_cd,
        dv_deal_id,
        dv_recurring_offer_cd
    FROM source_wi_pnl_rev_cost_msr_srvc_rev
)

SELECT * FROM final