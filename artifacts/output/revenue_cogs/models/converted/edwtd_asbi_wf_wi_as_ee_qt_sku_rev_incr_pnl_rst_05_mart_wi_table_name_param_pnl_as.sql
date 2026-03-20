{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_wi_as_ee_qt_sku_rev_incr_pnl_rst', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_WI_AS_EE_QT_SKU_REV_INCR_PNL_RST',
        'target_table': 'WI_TABLE_NAME_PARAM_PNL_AS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.722444+00:00'
    }
) }}

WITH 

source_wi_table_name_param_pnl_as AS (
    SELECT
        table_name1,
        table_name2
    FROM {{ source('raw', 'wi_table_name_param_pnl_as') }}
),

source_wi_soline_as_qt_drv_rev_incl AS (
    SELECT
        bk_as_quote_num,
        sales_order_line_key,
        sales_order_key,
        dv_bkgs_rev_cd,
        dv_qt_association_cd,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        bk_ss_cd
    FROM {{ source('raw', 'wi_soline_as_qt_drv_rev_incl') }}
),

source_wi_as_ee_qt_sku_rev_incr_pnl AS (
    SELECT
        dv_sales_order_line_key,
        bk_so_line_number_int,
        product_key,
        sales_territory_key,
        sales_rep_number,
        sales_order_key,
        bill_to_customer_key,
        end_customer_key,
        dv_fiscal_dt,
        dv_comp_us_net_rev_amt,
        sales_order_line_key,
        ship_to_customer_key,
        revenue_measure_key,
        rev_measure_trans_type_cd,
        dv_fiscal_year_mth_number_int,
        bk_product_subgroup_id,
        bk_allocated_servc_group_id,
        bk_product_id,
        ru_bk_product_family_id,
        ru_bk_service_prod_subgroup_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dd_bk_financial_account_cd,
        dv_attribution_cd,
        goods_or_service_type,
        dv_ar_trx_line_key,
        sold_to_customer_key,
        dv_corporate_revenue_flg,
        gl_dt,
        dv_recurring_offer_cd,
        dv_service_flg,
        gl_posted_dtm,
        dv_fmv_flg,
        dv_trans_type_category_cd,
        ar_trx_key,
        bk_ar_trx_line_gl_distrib_key,
        operating_unit_name_cd,
        bk_iso_country_code,
        bk_company_code,
        dv_erp_deal_id,
        dv_product_key,
        adjsmt_rbt_trx_lin_gl_dist_key,
        revenue_transfer_key,
        source_update_dtm,
        bk_service_category_id,
        parent_sku,
        sk_offer_attribution_id_int,
        dv_trx_fsc_yr_mth_wk_num_int,
        dv_bni_fhi_type_code,
        sk_sales_motion_attrib_key
    FROM {{ source('raw', 'wi_as_ee_qt_sku_rev_incr_pnl') }}
),

source_wi_as_ee_rev_enrichment_pnl AS (
    SELECT
        sales_order_key,
        bk_so_number_int,
        dv_sales_order_line_key,
        bk_so_line_number_int,
        sales_territory_key,
        bk_product_id,
        as_quote_num,
        bk_allocated_servc_group_id,
        bk_product_subgroup_id,
        ru_bk_service_prod_subgroup_id,
        bk_as_architecture_name,
        bk_as_technology_name,
        bk_as_subtechnology_name,
        bk_as_business_service_name,
        fiscal_year_month_int,
        raw_revenue_amt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        dv_qt_association_cd,
        ss_cd,
        dd_bk_financial_account_cd,
        dv_attribution_cd,
        goods_or_service_type,
        dv_ar_trx_line_key,
        sold_to_customer_key,
        dv_corporate_revenue_flg,
        gl_dt,
        dv_recurring_offer_cd,
        dv_service_flg,
        gl_posted_dtm,
        dv_fmv_flg,
        dv_trans_type_category_cd,
        revenue_measure_key,
        rev_measure_trans_type_cd,
        sales_order_line_key,
        bill_to_customer_key,
        end_customer_key,
        ship_to_customer_key,
        sales_rep_number,
        product_key,
        dv_fiscal_dt,
        ar_trx_key,
        bk_ar_trx_line_gl_distrib_key,
        operating_unit_name_cd,
        bk_iso_country_code,
        bk_company_code,
        dv_erp_deal_id,
        dv_product_key,
        adjsmt_rbt_trx_lin_gl_dist_key,
        revenue_transfer_key,
        bk_service_category_id,
        parent_sku,
        sk_offer_attribution_id_int,
        dv_trx_fsc_yr_mth_wk_num_int,
        dv_bni_fhi_type_code,
        sk_sales_motion_attrib_key
    FROM {{ source('raw', 'wi_as_ee_rev_enrichment_pnl') }}
),

final AS (
    SELECT
        table_name1,
        table_name2
    FROM source_wi_as_ee_rev_enrichment_pnl
)

SELECT * FROM final