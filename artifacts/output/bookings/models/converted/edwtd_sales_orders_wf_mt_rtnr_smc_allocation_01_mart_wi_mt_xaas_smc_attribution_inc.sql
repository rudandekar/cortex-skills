{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_rtnr_smc_allocation', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_RTNR_SMC_ALLOCATION',
        'target_table': 'WI_MT_XAAS_SMC_ATTRIBUTION_INC',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.395017+00:00'
    }
) }}

WITH 

source_wi_mt_xaas_smc_attribution_inc AS (
    SELECT
        sales_order_line_key,
        dv_enterprise_inv_sku_id,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        start_tv_dtm,
        end_tv_dtm,
        sales_motion_timing_cd,
        dv_source_type,
        renewal_gap_days,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_ref_rule
    FROM {{ source('raw', 'wi_mt_xaas_smc_attribution_inc') }}
),

source_wi_xaas_rtnr_smc_allocation AS (
    SELECT
        sales_order_line_key,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        start_tv_dtm,
        end_tv_dtm,
        sales_motion_timing_cd,
        dv_source_type,
        renewal_gap_days,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_ref_rule
    FROM {{ source('raw', 'wi_xaas_rtnr_smc_allocation') }}
),

source_mt_rtnr_smc_allocation AS (
    SELECT
        sales_order_line_key,
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
    FROM {{ source('raw', 'mt_rtnr_smc_allocation') }}
),

source_wi_rtnr_smc_allocation AS (
    SELECT
        sales_order_line_key,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        start_tv_dtm,
        end_tv_dtm,
        sales_motion_timing_cd,
        dv_source_type,
        renewal_gap_days,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_ref_rule
    FROM {{ source('raw', 'wi_rtnr_smc_allocation') }}
),

source_wi_mt_smc_attribution_incr AS (
    SELECT
        sales_order_line_key,
        dv_enterprise_inv_sku_id,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        start_tv_dtm,
        end_tv_dtm,
        sales_motion_timing_cd,
        dv_source_type,
        renewal_gap_days,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_ref_rule
    FROM {{ source('raw', 'wi_mt_smc_attribution_incr') }}
),

final AS (
    SELECT
        sales_order_line_key,
        dv_enterprise_inv_sku_id,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        start_tv_dtm,
        end_tv_dtm,
        sales_motion_timing_cd,
        dv_source_type,
        renewal_gap_days,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_ref_rule
    FROM source_wi_mt_smc_attribution_incr
)

SELECT * FROM final