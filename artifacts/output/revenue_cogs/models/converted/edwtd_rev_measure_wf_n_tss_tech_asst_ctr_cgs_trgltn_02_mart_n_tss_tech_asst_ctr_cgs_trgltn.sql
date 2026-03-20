{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_tss_tech_asst_ctr_cgs_trgltn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_TSS_TECH_ASST_CTR_CGS_TRGLTN',
        'target_table': 'N_TSS_TECH_ASST_CTR_CGS_TRGLTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.088994+00:00'
    }
) }}

WITH 

source_n_tss_tech_asst_ctr_cgs_trgltn AS (
    SELECT
        original_sales_territory_key,
        triangulated_sales_terr_key,
        bk_src_rprtd_srvc_contract_num,
        service_product_key,
        goods_product_key,
        bk_triangulation_type_cd,
        bk_c3_customer_theater_name,
        bk_c3_cust_market_segment_name,
        bk_src_reported_tac_country_cd,
        tac_backbone_cost_usd_amt,
        tac_thtr_buz_oprtn_cst_usd_amt,
        tac_out_tasking_cost_amt,
        tac_overhead_cost_usd_amt,
        tac_warranty_percent,
        tac_adjusted_overhead_pct,
        bk_st_enrchmnt_mthdlgy_cd,
        service_contract_num,
        bk_fiscal_year_mth_number_int,
        service_request_tac_cost_key,
        work_time_in_mins_cnt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM {{ source('raw', 'n_tss_tech_asst_ctr_cgs_trgltn') }}
),

source_wi_ro_key_ts_tch_ast_ctr_cgs AS (
    SELECT
        dv_product_key,
        product_key,
        sales_order_key,
        sk_offer_attribution_id_int,
        dv_recurring_offer_cd
    FROM {{ source('raw', 'wi_ro_key_ts_tch_ast_ctr_cgs') }}
),

source_w_tss_tech_asst_ctr_cgs_trgltn AS (
    SELECT
        original_sales_territory_key,
        triangulated_sales_terr_key,
        bk_src_rprtd_srvc_contract_num,
        service_product_key,
        goods_product_key,
        bk_triangulation_type_cd,
        bk_c3_customer_theater_name,
        bk_c3_cust_market_segment_name,
        bk_src_reported_tac_country_cd,
        tac_backbone_cost_usd_amt,
        tac_thtr_buz_oprtn_cst_usd_amt,
        tac_out_tasking_cost_amt,
        tac_overhead_cost_usd_amt,
        tac_warranty_percent,
        tac_adjusted_overhead_pct,
        bk_st_enrchmnt_mthdlgy_cd,
        service_contract_num,
        bk_fiscal_year_mth_number_int,
        service_request_tac_cost_key,
        work_time_in_mins_cnt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_tss_tech_asst_ctr_cgs_trgltn') }}
),

final AS (
    SELECT
        original_sales_territory_key,
        triangulated_sales_terr_key,
        bk_src_rprtd_srvc_contract_num,
        service_product_key,
        goods_product_key,
        bk_triangulation_type_cd,
        bk_c3_customer_theater_name,
        bk_c3_cust_market_segment_name,
        bk_src_reported_tac_country_cd,
        tac_backbone_cost_usd_amt,
        tac_thtr_buz_oprtn_cst_usd_amt,
        tac_out_tasking_cost_amt,
        tac_overhead_cost_usd_amt,
        tac_warranty_percent,
        tac_adjusted_overhead_pct,
        bk_st_enrchmnt_mthdlgy_cd,
        service_contract_num,
        bk_fiscal_year_mth_number_int,
        service_request_tac_cost_key,
        work_time_in_mins_cnt,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_w_tss_tech_asst_ctr_cgs_trgltn
)

SELECT * FROM final