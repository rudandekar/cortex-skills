{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_tss_srvc_sprt_ctr_wc_trgltn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_TSS_SRVC_SPRT_CTR_WC_TRGLTN',
        'target_table': 'EX_AE_SVC_SSC_WC_TRIANG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.471315+00:00'
    }
) }}

WITH 

source_wi_tss_srvc_sprt_ctr_wc_trgltn AS (
    SELECT
        original_sales_territory_key,
        triangulated_sales_terr_key,
        bk_src_rptd_srvc_cntrct_num,
        service_prdt_key,
        goods_prdt_key,
        bk_triangulation_type_cd,
        bk_c3_cust_theater_name,
        bk_c3_cust_mrkt_segment_name,
        bk_wc_as_of_fiscal_cal_cd,
        bk_wc_as_of_fiscal_yr_num_int,
        bk_wc_as_of_fscl_mth_num_int,
        bk_service_contract_num,
        bk_sls_terr_enrchmt_mthdlgy_cd,
        ssc_warranty_credit_usd_amt,
        service_request_ssc_cost_key
    FROM {{ source('raw', 'wi_tss_srvc_sprt_ctr_wc_trgltn') }}
),

source_st_ae_svc_ssc_cogs_triang AS (
    SELECT
        sales_territory_code,
        drv_sales_territory_code,
        service_product_id,
        goods_product_id,
        fiscal_month_id,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        ssc_dpt_prsnl_rsrc_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_thrd_prty_mtnc_cst_usd_amt,
        ssc_orig_eqp_mfgr_cst_usd_amt,
        dv_ssc_warranty_pct,
        contract_number,
        enrichment_methodolody_desc,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        behavior_id,
        triangulation_type_id,
        sub_measure_key,
        ssc_warranty_credit_usd_amt,
        service_request_ssc_cost_key
    FROM {{ source('raw', 'st_ae_svc_ssc_cogs_triang') }}
),

final AS (
    SELECT
        sales_territory_code,
        drv_sales_territory_code,
        service_product_id,
        goods_product_id,
        fiscal_month_id,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        ssc_dpt_prsnl_rsrc_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_thrd_prty_mtnc_cst_usd_amt,
        ssc_orig_eqp_mfgr_cst_usd_amt,
        dv_ssc_warranty_pct,
        contract_number,
        enrichment_methodolody_desc,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        behavior_id,
        triangulation_type_id,
        sub_measure_key,
        exception_type,
        ssc_warranty_credit_usd_amt,
        service_request_ssc_cost_key
    FROM source_st_ae_svc_ssc_cogs_triang
)

SELECT * FROM final