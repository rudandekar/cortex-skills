{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_tss_tech_asst_ctr_cgs_trgltn', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_TSS_TECH_ASST_CTR_CGS_TRGLTN',
        'target_table': 'EX_AE_SVC_TAC_COGS_TRIANG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.568933+00:00'
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
        exception_type,
        service_request_tac_cost_key
    FROM source_st_ae_svc_tac_cogs_triang
)

SELECT * FROM final