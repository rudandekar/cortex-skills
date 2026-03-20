{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_inv_rev_pf_qtr_ind_adj_me', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_INV_REV_PF_QTR_IND_ADJ_ME',
        'target_table': 'MT_INV_REVENUE_PF_QTR_ME',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.458584+00:00'
    }
) }}

WITH 

source_wi_inv_rev_pf_qtr_ind_adj_me AS (
    SELECT
        sales_order_key,
        ru_bk_product_family_id,
        ar_trx_key,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        sales_territory_key,
        fiscal_year_quarter_number_int,
        dd_bk_financial_account_cd,
        rev_measure_trans_type_cd,
        dv_corporate_revenue_flg,
        dv_ic_revenue_flg,
        dv_charges_flg,
        dv_misc_flg,
        dv_service_flg,
        dv_international_demo_flg,
        dv_replacement_demo_flg,
        dv_revenue_flg,
        transaction_type_category_cd,
        warehouse_inventory_org_key,
        l3_hierarchy_node_type_id,
        l2_hierarchy_node_type_id,
        l1_hierarchy_node_type_id,
        dv_comp_us_net_price_amt,
        dv_comp_us_net_list_price_amt,
        dv_comp_us_gross_list_price_am,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_rev_amt,
        dv_comp_us_net_rev_amt,
        dv_comp_us_2tier_cmdm_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_standard_price_amt,
        dd_extended_net_qty,
        dd_extended_gross_qty,
        gl_distrib_functional_amt,
        gl_distrib_transactional_amt,
        dv_extended_qty
    FROM {{ source('raw', 'wi_inv_rev_pf_qtr_ind_adj_me') }}
),

final AS (
    SELECT
        sales_order_key,
        ru_bk_product_family_id,
        ar_trx_key,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        sales_territory_key,
        fiscal_year_quarter_number_int,
        dd_bk_financial_account_cd,
        rev_measure_trans_type_cd,
        dv_corporate_revenue_flg,
        dv_ic_revenue_flg,
        dv_charges_flg,
        dv_misc_flg,
        dv_service_flg,
        dv_international_demo_flg,
        dv_replacement_demo_flg,
        dv_revenue_flg,
        transaction_type_category_cd,
        warehouse_inventory_org_key,
        l3_hierarchy_node_type_id,
        l2_hierarchy_node_type_id,
        l1_hierarchy_node_type_id,
        dv_comp_us_net_price_amt,
        dv_comp_us_net_list_price_amt,
        dv_comp_us_gross_list_price_am,
        dv_comp_us_net_cost_amt,
        dv_comp_us_gross_rev_amt,
        dv_comp_us_net_rev_amt,
        dv_comp_us_2tier_cmdm_amt,
        dv_comp_us_gross_cost_amt,
        dv_comp_us_standard_price_amt,
        dd_extended_net_qty,
        dd_extended_gross_qty,
        gl_distrib_functional_amt,
        gl_distrib_transactional_amt,
        dv_extended_qty
    FROM source_wi_inv_rev_pf_qtr_ind_adj_me
)

SELECT * FROM final