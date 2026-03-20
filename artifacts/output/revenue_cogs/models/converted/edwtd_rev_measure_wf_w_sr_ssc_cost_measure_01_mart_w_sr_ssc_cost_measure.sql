{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_sr_ssc_cost_measure', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_SR_SSC_COST_MEASURE',
        'target_table': 'W_SR_SSC_COST_MEASURE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.725702+00:00'
    }
) }}

WITH 

source_wi_ccm_ssc_id_service_key AS (
    SELECT
        fiscal_quarter_name,
        service_request_ssc_cost_key,
        service_product_key
    FROM {{ source('raw', 'wi_ccm_ssc_id_service_key') }}
),

source_wi_ccm_ssc_id_product_key AS (
    SELECT
        fiscal_quarter_name,
        service_request_ssc_cost_key,
        goods_product_key
    FROM {{ source('raw', 'wi_ccm_ssc_id_product_key') }}
),

source_n_sr_srvc_support_center_cost AS (
    SELECT
        service_request_ssc_cost_key,
        src_rptd_sr_number_lint,
        src_rptd_service_contarct_num,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        sr_product_subgroup_id,
        sr_allocated_service_group_id,
        sr_service_category_id,
        sr_product_family_id,
        ssc_dpt_prsnl_rsrc_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_thrd_prty_mtnc_cst_usd_amt,
        ssc_orig_eqp_mfgr_cst_usd_amt,
        dv_ssc_warranty_pct,
        sk_ccm_ssc_id_int,
        fiscal_year_month_int,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_sr_srvc_support_center_cost') }}
),

source_wi_ccm_ssc_id_sales_terr_key AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        service_request_ssc_cost_key,
        bk_repair_order_num_int,
        src_rptd_sr_number_int,
        dv_end_customer_name,
        drvd_sales_territory_key,
        sa_slk_allocation_ratio,
        allocation_type,
        rma_cost
    FROM {{ source('raw', 'wi_ccm_ssc_id_sales_terr_key') }}
),

source_wi_ccm_ts_mth_qtr_cntl AS (
    SELECT
        fiscal_quarter_name,
        fiscal_year_month_int,
        ccm_fiscal_month,
        month_rank
    FROM {{ source('raw', 'wi_ccm_ts_mth_qtr_cntl') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_wi_ccm_ts_mth_qtr_cntl
)

SELECT * FROM final