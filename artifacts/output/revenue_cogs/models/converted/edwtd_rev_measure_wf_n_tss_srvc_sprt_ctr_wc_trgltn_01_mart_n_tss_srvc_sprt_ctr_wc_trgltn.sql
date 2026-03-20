{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_tss_srvc_sprt_ctr_wc_trgltn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_TSS_SRVC_SPRT_CTR_WC_TRGLTN',
        'target_table': 'N_TSS_SRVC_SPRT_CTR_WC_TRGLTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.360322+00:00'
    }
) }}

WITH 

source_w_tss_srvc_sprt_ctr_wc_trgltn AS (
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
        service_request_ssc_cost_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_tss_srvc_sprt_ctr_wc_trgltn') }}
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
        bk_wc_as_of_fiscal_cal_cd,
        bk_wc_as_of_fiscal_yr_num_int,
        bk_wc_as_of_fscl_mth_num_int,
        bk_service_contract_num,
        bk_sls_terr_enrchmt_mthdlgy_cd,
        ssc_warranty_credit_usd_amt,
        service_request_ssc_cost_key,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_tss_srvc_sprt_ctr_wc_trgltn
)

SELECT * FROM final