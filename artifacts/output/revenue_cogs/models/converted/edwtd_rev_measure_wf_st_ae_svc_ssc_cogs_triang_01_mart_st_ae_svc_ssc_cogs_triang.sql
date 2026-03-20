{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_svc_ssc_cogs_triang', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_AE_SVC_SSC_COGS_TRIANG',
        'target_table': 'ST_AE_SVC_SSC_COGS_TRIANG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.563640+00:00'
    }
) }}

WITH 

source_ff_ae_svc_ssc_cogs_triang AS (
    SELECT
        fiscal_month_id,
        measure_id,
        sub_measure_key,
        service_request_ssc_cost_key,
        sales_territory_code,
        drv_sales_territory_code,
        service_product_id,
        goods_product_id,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        enrichment_methodolody_desc,
        contract_number,
        ssc_dpt_prsnl_rsrc_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_thrd_prty_mtnc_cst_usd_amt,
        ssc_orig_eqp_mfgr_cst_usd_amt,
        dv_ssc_warranty_pct,
        ssc_warranty_credit_usd_amt,
        triangulation_type_id
    FROM {{ source('raw', 'ff_ae_svc_ssc_cogs_triang') }}
),

final AS (
    SELECT
        fiscal_month_id,
        measure_id,
        sub_measure_key,
        service_request_ssc_cost_key,
        sales_territory_code,
        drv_sales_territory_code,
        service_product_id,
        goods_product_id,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        enrichment_methodolody_desc,
        contract_number,
        ssc_dpt_prsnl_rsrc_cst_usd_amt,
        ssc_depreciation_cost_usd_amt,
        ssc_repair_cost_usd_amt,
        ssc_duty_vat_cost_usd_amt,
        ssc_thrd_prty_lgst_cst_usd_amt,
        ssc_thrd_prty_mtnc_cst_usd_amt,
        ssc_orig_eqp_mfgr_cst_usd_amt,
        dv_ssc_warranty_pct,
        ssc_warranty_credit_usd_amt,
        triangulation_type_id
    FROM source_ff_ae_svc_ssc_cogs_triang
)

SELECT * FROM final