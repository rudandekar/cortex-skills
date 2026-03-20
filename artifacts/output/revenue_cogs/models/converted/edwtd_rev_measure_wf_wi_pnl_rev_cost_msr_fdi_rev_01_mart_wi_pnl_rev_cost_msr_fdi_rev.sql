{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_pnl_rev_cost_msr_fdi_rev', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_PNL_REV_COST_MSR_FDI_REV',
        'target_table': 'WI_PNL_REV_COST_MSR_FDI_REV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.434083+00:00'
    }
) }}

WITH 

source_wi_pnl_rev_cost_msr_fdi_rev AS (
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
        ce_country_name,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_pnl_rev_cost_msr_fdi_rev') }}
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
        ce_country_name,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_recurring_offer_cd
    FROM source_wi_pnl_rev_cost_msr_fdi_rev
)

SELECT * FROM final