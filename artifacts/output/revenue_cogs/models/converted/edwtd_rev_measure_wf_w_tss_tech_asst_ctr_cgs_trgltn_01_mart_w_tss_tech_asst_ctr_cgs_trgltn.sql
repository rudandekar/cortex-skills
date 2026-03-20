{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_tss_tech_asst_ctr_cgs_trgltn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_TSS_TECH_ASST_CTR_CGS_TRGLTN',
        'target_table': 'W_TSS_TECH_ASST_CTR_CGS_TRGLTN',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.139992+00:00'
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
        service_request_tac_cost_key
    FROM {{ source('raw', 'st_ae_svc_tac_cogs_triang') }}
),

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
        allocation_trans_id,
        triangulation_type_id,
        sub_measure_key,
        service_request_tac_cost_key
    FROM {{ source('raw', 'st_ae_svc_tac_cogs_triang') }}
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
        edw_update_user,
        action_code,
        dml_type
    FROM source_st_ae_svc_tac_cogs_triang
)

SELECT * FROM final