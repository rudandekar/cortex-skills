{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_rev_cost_msr_cis_cogs', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_REV_COST_MSR_CIS_COGS',
        'target_table': 'WI_PNL_REV_COST_MSR_CIS_COGS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.321039+00:00'
    }
) }}

WITH 

source_wi_pnl_rev_cost_msr_cis_cogs AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_revenue_or_cogs_type_cd,
        bk_product_key,
        goods_product_key,
        bk_financial_account_cd,
        bk_rev_measure_trnsctn_type_cd,
        dv_triangulation_type_id_int,
        bk_prdt_subgroup_alloc_src_cd,
        bk_sales_territory_key,
        bk_trngltd_sales_territory_key,
        sales_order_line_key,
        bk_src_rprtd_srvc_contract_num,
        comp_us_net_rev_amt,
        bk_company_cd,
        bk_iso_country_cd,
        dv_product_type,
        dv_pnl_line_item_name,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr_cis_cogs') }}
),

final AS (
    SELECT
        bk_fiscal_year_mth_number_int,
        bk_revenue_or_cogs_type_cd,
        bk_product_key,
        goods_product_key,
        bk_financial_account_cd,
        bk_rev_measure_trnsctn_type_cd,
        dv_triangulation_type_id_int,
        bk_prdt_subgroup_alloc_src_cd,
        bk_sales_territory_key,
        bk_trngltd_sales_territory_key,
        sales_order_line_key,
        bk_src_rprtd_srvc_contract_num,
        comp_us_net_rev_amt,
        bk_company_cd,
        bk_iso_country_cd,
        dv_product_type,
        dv_pnl_line_item_name,
        dv_recurring_offer_cd
    FROM source_wi_pnl_rev_cost_msr_cis_cogs
)

SELECT * FROM final