{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_service_revenue_by_pid_20range_left', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_WI_SERVICE_REVENUE_BY_PID_20RANGE_LEFT',
        'target_table': 'WI_SERVICE_REVENUE_PP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.286796+00:00'
    }
) }}

WITH 

source_wi_service_revenue_pp AS (
    SELECT
        revenue_measure_key,
        rev_measure_trans_type_cd,
        sales_rep_number,
        sales_territory_key,
        dv_fiscal_year_mth_number_int,
        sales_order_key,
        sales_order_line_key,
        dv_sales_order_line_key,
        sk_offer_attribution_id_int,
        dv_attribution_cd,
        so_number_int,
        product_key,
        dv_product_key,
        goods_or_service_type,
        ru_bk_product_subgroup_id,
        service_sku,
        ru_bk_product_family_id,
        bk_allocated_servc_group_id,
        dv_comp_us_net_rev_amt,
        dv_comp_func_net_rev_amt,
        gl_distrib_transactional_amt,
        gl_distrib_functional_amt,
        dv_func_ext_invoice_amount,
        dv_usd_ext_invoice_amount,
        dv_func_shipped_revenue_amt,
        dv_shipped_revenue_amt
    FROM {{ source('raw', 'wi_service_revenue_pp') }}
),

source_wi_service_revenue_by_pid AS (
    SELECT
        revenue_measure_key,
        rev_measure_trans_type_cd,
        sales_rep_number,
        sales_territory_key,
        dv_fiscal_year_mth_number_int,
        sales_order_key,
        sales_order_line_key,
        dv_sales_order_line_key,
        sk_offer_attribution_id_int,
        dv_attribution_cd,
        so_number_int,
        product_key,
        dv_product_key,
        goods_or_service_type,
        ru_bk_product_subgroup_id,
        service_sku,
        ru_bk_product_family_id,
        goods_product_key,
        bk_allocated_servc_group_id,
        rev_alloc_pct,
        bkgs_alloc_pct,
        metric_type,
        dv_comp_us_net_rev_amt,
        dv_comp_func_net_rev_amt,
        gl_distrib_transactional_amt,
        gl_distrib_functional_amt,
        dv_func_ext_invoice_amount,
        dv_usd_ext_invoice_amount,
        dv_func_shipped_revenue_amt,
        dv_shipped_revenue_amt
    FROM {{ source('raw', 'wi_service_revenue_by_pid') }}
),

final AS (
    SELECT
        revenue_measure_key,
        rev_measure_trans_type_cd,
        sales_rep_number,
        sales_territory_key,
        dv_fiscal_year_mth_number_int,
        sales_order_key,
        sales_order_line_key,
        dv_sales_order_line_key,
        sk_offer_attribution_id_int,
        dv_attribution_cd,
        so_number_int,
        product_key,
        dv_product_key,
        goods_or_service_type,
        ru_bk_product_subgroup_id,
        service_sku,
        ru_bk_product_family_id,
        bk_allocated_servc_group_id,
        dv_comp_us_net_rev_amt,
        dv_comp_func_net_rev_amt,
        gl_distrib_transactional_amt,
        gl_distrib_functional_amt,
        dv_func_ext_invoice_amount,
        dv_usd_ext_invoice_amount,
        dv_func_shipped_revenue_amt,
        dv_shipped_revenue_amt
    FROM source_wi_service_revenue_by_pid
)

SELECT * FROM final