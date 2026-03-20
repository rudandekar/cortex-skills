{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_tss_srvc_sprt_ctr_cgs_trgltn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_TSS_SRVC_SPRT_CTR_CGS_TRGLTN',
        'target_table': 'EX_AE_SVC_SSC_COGS_TRIANG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.634724+00:00'
    }
) }}

WITH 

source_wi_tss_srvc_sprt_ctr_cgs AS (
    SELECT
        original_sales_territory_key,
        triangulated_sales_terr_key,
        bk_src_rprtd_srvc_contract_num,
        service_product_key,
        goods_product_key,
        bk_triangulation_type_cd,
        bk_c3_customer_theater_name,
        bk_c3_cust_market_segment_name,
        ssc_dpt_psnl_rsrc_cost_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_trdprty_lgsts_cost_usd_amt,
        ssc_trdprty_mntnc_cost_usd_amt,
        ssc_orig_eqpt_mfg_cost_usd_amt,
        ssc_warranty_pct,
        service_contract_num,
        bk_st_enrchmnt_mthdlgy_cd,
        bk_fiscal_year_mth_number_int,
        service_request_ssc_cost_key
    FROM {{ source('raw', 'wi_tss_srvc_sprt_ctr_cgs') }}
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
        service_request_ssc_cost_key
    FROM source_st_ae_svc_ssc_cogs_triang
)

SELECT * FROM final