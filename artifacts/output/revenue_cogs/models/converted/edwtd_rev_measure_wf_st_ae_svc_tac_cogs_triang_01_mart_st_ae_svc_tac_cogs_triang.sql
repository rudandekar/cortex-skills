{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ae_svc_tac_cogs_triang', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_AE_SVC_TAC_COGS_TRIANG',
        'target_table': 'ST_AE_SVC_TAC_COGS_TRIANG',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.886065+00:00'
    }
) }}

WITH 

source_ff_ae_svc_tac_cogs_triang AS (
    SELECT
        fiscal_month_id,
        measure_id,
        sub_measure_key,
        service_request_tac_cost_key,
        sales_territory_code,
        drv_sales_territory_code,
        service_product_id,
        goods_product_id,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        enrichment_methodolody_desc,
        contract_number,
        source_reported_tac_country_cd,
        tac_backbone_cost_usd_amt,
        tac_thtr_biz_oprtn_cst_usd_amt,
        tac_out_tasking_cost_amt,
        tac_overhead_cost_usd_amt,
        dv_tac_warranty_pct,
        dv_tac_adjusted_overhead_pct,
        tac_warranty_credit_usd_amt,
        triangulation_type_id
    FROM {{ source('raw', 'ff_ae_svc_tac_cogs_triang') }}
),

final AS (
    SELECT
        fiscal_month_id,
        measure_id,
        sub_measure_key,
        service_request_tac_cost_key,
        sales_territory_code,
        drv_sales_territory_code,
        service_product_id,
        goods_product_id,
        c3_customer_theater_name,
        c3_cust_market_segment_name,
        enrichment_methodolody_desc,
        contract_number,
        source_reported_tac_country_cd,
        tac_backbone_cost_usd_amt,
        tac_thtr_biz_oprtn_cst_usd_amt,
        tac_out_tasking_cost_amt,
        tac_overhead_cost_usd_amt,
        dv_tac_warranty_pct,
        dv_tac_adjusted_overhead_pct,
        tac_warranty_credit_usd_amt,
        triangulation_type_id
    FROM source_ff_ae_svc_tac_cogs_triang
)

SELECT * FROM final