{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_tss_tech_asst_ctr_wc_trgltn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_TSS_TECH_ASST_CTR_WC_TRGLTN',
        'target_table': 'WI_TSS_TECH_ASST_CTR_WC_TRGLTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.801122+00:00'
    }
) }}

WITH 

source_st_ae_svc_tac_cogs_triang AS (
    SELECT
        sales_territory_code,
        drv_sales_territory_code,
        service_product_id,
        goods_product_id,
        fiscal_month_id,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        tac_backbone_cost_usd_amt,
        tac_thtr_biz_oprtn_cst_usd_amt,
        tac_out_tasking_cost_amt,
        tac_overhead_cost_usd_amt,
        dv_tac_warranty_pct,
        dv_tac_adjusted_overhead_pct,
        source_reported_tac_country_cd,
        contract_number,
        enrichment_methodolody_desc,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        behavior_id,
        triangulation_type_id,
        sub_measure_key,
        tac_warranty_credit_usd_amt,
        service_request_tac_cost_key
    FROM {{ source('raw', 'st_ae_svc_tac_cogs_triang') }}
),

source_wi_tss_tech_asst_ctr_wc_trgltn AS (
    SELECT
        original_sales_territory_key,
        triangulated_sales_terr_key,
        bk_src_rptd_srvc_cntrct_num,
        service_prdt_key,
        goods_prdt_key,
        bk_triangulation_type_cd,
        bk_c3_cust_theater_name,
        bk_c3_cust_mrkt_segment_name,
        bk_src_rptd_tac_cntry_cd,
        bk_wc_as_of_fiscal_cal_cd,
        bk_wc_as_of_fiscal_yr_num_int,
        bk_wc_as_of_fscl_mth_num_int,
        bk_sls_terr_enrchmt_mthdlgy_cd,
        bk_service_contract_num,
        tac_warranty_credit_usd_amt,
        service_request_tac_cost_key
    FROM {{ source('raw', 'wi_tss_tech_asst_ctr_wc_trgltn') }}
),

final AS (
    SELECT
        original_sales_territory_key,
        triangulated_sales_terr_key,
        bk_src_rptd_srvc_cntrct_num,
        service_prdt_key,
        goods_prdt_key,
        bk_triangulation_type_cd,
        bk_c3_cust_theater_name,
        bk_c3_cust_mrkt_segment_name,
        bk_src_rptd_tac_cntry_cd,
        bk_wc_as_of_fiscal_cal_cd,
        bk_wc_as_of_fiscal_yr_num_int,
        bk_wc_as_of_fscl_mth_num_int,
        bk_sls_terr_enrchmt_mthdlgy_cd,
        bk_service_contract_num,
        tac_warranty_credit_usd_amt,
        service_request_tac_cost_key
    FROM source_wi_tss_tech_asst_ctr_wc_trgltn
)

SELECT * FROM final