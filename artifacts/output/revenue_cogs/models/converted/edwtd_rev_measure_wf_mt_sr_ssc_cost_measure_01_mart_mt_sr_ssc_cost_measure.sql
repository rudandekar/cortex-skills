{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sr_ssc_cost_measure', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_MT_SR_SSC_COST_MEASURE',
        'target_table': 'MT_SR_SSC_COST_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.899043+00:00'
    }
) }}

WITH 

source_w_sr_ssc_cost_measure AS (
    SELECT
        service_request_ssc_cost_key,
        sales_territory_key,
        service_product_key,
        goods_product_key,
        fiscal_year_month_int,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        src_rptd_service_contarct_num,
        ssc_dpt_prsnl_rsrc_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_thrd_prty_mtnc_cst_usd_amt,
        ssc_orig_eqp_mfgr_cst_usd_amt,
        dv_ssc_warranty_pct,
        enrichment_methodolody_desc,
        shipped_qty,
        contracted_service_group_name,
        src_rptd_site_cntry_name,
        src_rptd_cust_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_sr_ssc_cost_measure') }}
),

final AS (
    SELECT
        service_request_ssc_cost_key,
        sales_territory_key,
        service_product_key,
        goods_product_key,
        fiscal_year_month_int,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        src_rptd_service_contarct_num,
        ssc_dpt_prsnl_rsrc_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_thrd_prty_mtnc_cst_usd_amt,
        ssc_orig_eqp_mfgr_cst_usd_amt,
        dv_ssc_warranty_pct,
        enrichment_methodolody_desc,
        shipped_qty,
        contracted_service_group_name,
        src_rptd_site_cntry_name,
        src_rptd_cust_name,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_sr_ssc_cost_measure
)

SELECT * FROM final