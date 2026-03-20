{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_pnl_rev_cost_measure_srvc_rev_rtr', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_PNL_REV_COST_MEASURE_SRVC_REV_RTR',
        'target_table': 'MT_PNL_REV_COST_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.159348+00:00'
    }
) }}

WITH 

source_mt_pnl_rev_cost_measure AS (
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
        original_sales_territory_key,
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_pnl_rev_cost_type,
        deal_ss_cd,
        dv_deal_id
    FROM {{ source('raw', 'mt_pnl_rev_cost_measure') }}
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
        original_sales_territory_key,
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
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_pnl_rev_cost_type,
        deal_ss_cd,
        dv_deal_id,
        xcat_flg,
        bk_offer_type_name,
        recurring_offer_flg,
        ela_flg,
        dv_recurring_offer_cd,
        tsv_accruals_cd
    FROM source_mt_pnl_rev_cost_measure
)

SELECT * FROM final