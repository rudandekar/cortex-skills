{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_as_svcs_revenue_rst', 'batch', 'edwtd_asbi'],
    meta={
        'source_workflow': 'wf_m_MT_AS_SVCS_REVENUE_RST',
        'target_table': 'WI_TABLE_NAME_PARAM_PNL_AS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.302477+00:00'
    }
) }}

WITH 

source_wi_table_name_param_pnl_as AS (
    SELECT
        table_name1,
        table_name2
    FROM {{ source('raw', 'wi_table_name_param_pnl_as') }}
),

source_wi_as_success_track AS (
    SELECT
        product_key,
        dv_product_key,
        ru_bk_service_prod_subgroup_id,
        csp_cx_product,
        revenue_measure_key,
        rev_measure_trans_type_cd,
        sales_territory_key,
        sales_rep_num
    FROM {{ source('raw', 'wi_as_success_track') }}
),

source_wi_mt_as_svcs_revenue AS (
    SELECT
        revenue_measure_key,
        fiscal_year_month_int,
        dv_fiscal_dt,
        sales_order_key,
        sales_order_line_key,
        dv_sales_order_line_key,
        product_key,
        sales_territory_key,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        dv_ar_trx_line_key,
        sales_rep_number,
        dv_trans_type_category_cd,
        rev_measure_trans_type_cd,
        dd_bk_financial_account_cd,
        dv_attribution_cd,
        dv_recurring_offer_cd,
        dv_corporate_revenue_flg,
        dv_service_flg,
        dv_fmv_flg,
        goods_or_service_type,
        gl_posted_dtm,
        gl_dt,
        dv_goods_adj_prd_key,
        dv_bk_as_ato_archi_name,
        dv_bk_busi_svc_offer_type_name,
        dv_bk_as_ato_tech_name,
        ar_trx_key,
        ar_trx_line_gl_distrib_key,
        operating_unit_name_cd,
        iso_country_code,
        company_code,
        dv_erp_deal_id,
        dv_product_key,
        adjsmt_rbt_trx_lin_gl_dist_key,
        revenue_transfer_key,
        fiscal_year_quarter_number_int,
        dv_qt_association_cd,
        bk_allocated_servc_group_id,
        bk_service_category_id,
        comp_us_net_rev_amt,
        ru_bk_service_prod_subgroup_id,
        l3_sales_territory_name_code,
        sk_offer_attribution_id_int,
        package,
        dv_trx_fsc_yr_mth_wk_num_int,
        dv_bni_fhi_type_code,
        sk_sales_motion_attrib_key
    FROM {{ source('raw', 'wi_mt_as_svcs_revenue') }}
),

source_mt_as_svcs_revenue AS (
    SELECT
        revenue_measure_key,
        fiscal_year_mth_number_int,
        dv_fiscal_dt,
        sales_order_key,
        sales_order_line_key,
        dv_sales_order_line_key,
        product_key,
        sales_territory_key,
        end_customer_key,
        bill_to_customer_key,
        ship_to_customer_key,
        sold_to_customer_key,
        ar_trx_line_key,
        sales_rep_num,
        dv_trans_type_category_cd,
        rev_measure_trans_type_cd,
        bk_financial_account_cd,
        dv_attribution_cd,
        recurring_offer_cd,
        corporate_revenue_flg,
        service_flg,
        fmv_flg,
        goods_or_service_type,
        gl_posted_dtm,
        gl_dt,
        dv_goods_adj_prd_key,
        bk_as_ato_architecture_name,
        bk_busi_svc_offer_type_name,
        bk_as_ato_tech_name,
        comp_us_net_rev_amt,
        data_set_cd,
        be_sbe_allocation_rule,
        ar_trx_key,
        ar_trx_line_gl_distrib_key,
        operating_unit_name_cd,
        iso_country_code,
        company_code,
        dv_erp_deal_id,
        dv_product_key,
        adjsmt_rbt_trx_lin_gl_dist_key,
        revenue_transfer_key,
        dv_qt_association_cd,
        edw_update_user,
        edw_create_dtm,
        edw_update_dtm,
        edw_create_user,
        data_source_name,
        bk_allocated_servc_group_id,
        bk_service_category_id,
        dv_cx_product,
        dv_trx_fsc_yr_mth_wk_num_int,
        dv_bni_fhi_type_code,
        sk_sales_motion_attrib_key
    FROM {{ source('raw', 'mt_as_svcs_revenue') }}
),

final AS (
    SELECT
        table_name1,
        table_name2
    FROM source_mt_as_svcs_revenue
)

SELECT * FROM final