{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sol_prdt_smc_allocation', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_SOL_PRDT_SMC_ALLOCATION',
        'target_table': 'MT_SOL_PRDT_SMC_ALLOCATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.609171+00:00'
    }
) }}

WITH 

source_mt_sol_prdt_smc_allocation AS (
    SELECT
        sales_order_line_key,
        product_key,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sales_motion_timing_cd,
        dv_source_type,
        dv_trx_type,
        ss_code,
        renewal_gap_days,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_ref_rule
    FROM {{ source('raw', 'mt_sol_prdt_smc_allocation') }}
),

source_wi_erp_prdt_smc_allocation AS (
    SELECT
        sales_order_line_key,
        attributed_product_key,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        start_tv_dtm,
        end_tv_dtm,
        sales_motion_timing_cd,
        dv_source_type
    FROM {{ source('raw', 'wi_erp_prdt_smc_allocation') }}
),

source_wi_xaas_prdt_smc_allocation AS (
    SELECT
        sales_order_line_key,
        attributed_product_key,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        start_tv_dtm,
        end_tv_dtm,
        sales_motion_timing_cd,
        dv_source_type
    FROM {{ source('raw', 'wi_xaas_prdt_smc_allocation') }}
),

final AS (
    SELECT
        sales_order_line_key,
        product_key,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        start_tv_dtm,
        end_tv_dtm,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sales_motion_timing_cd,
        dv_source_type,
        dv_trx_type,
        ss_code,
        renewal_gap_days,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_ref_rule
    FROM source_wi_xaas_prdt_smc_allocation
)

SELECT * FROM final