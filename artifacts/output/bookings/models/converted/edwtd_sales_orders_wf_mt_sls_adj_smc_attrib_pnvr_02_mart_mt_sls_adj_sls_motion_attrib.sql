{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_sls_adj_smc_attrib_pnvr', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_SLS_ADJ_SMC_ATTRIB_PNVR',
        'target_table': 'MT_SLS_ADJ_SLS_MOTION_ATTRIB',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:34.381154+00:00'
    }
) }}

WITH 

source_wi_mt_sls_adj_smc_attrib_pnvr AS (
    SELECT
        manual_trx_key,
        sales_order_line_key,
        dv_enterprise_inv_sku_id,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        dv_oa_flg,
        start_tv_dtm,
        end_tv_dtm,
        dv_source_type,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        reason_code,
        sales_motion_timing,
        txn_cr_party_id,
        hq_cr_party_id,
        ar_trx_line_key,
        sales_motion_timing_cd,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_gap_days,
        renewal_ref_rule
    FROM {{ source('raw', 'wi_mt_sls_adj_smc_attrib_pnvr') }}
),

source_mt_sls_adj_sls_motion_attrib AS (
    SELECT
        manual_trx_key,
        sales_order_line_key,
        dv_enterprise_inv_sku_id,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        dv_oa_flg,
        start_tv_dtm,
        end_tv_dtm,
        dv_source_type,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sales_motion_timing_cd,
        sk_offer_attribution_id_int,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_gap_days,
        renewal_ref_rule
    FROM {{ source('raw', 'mt_sls_adj_sls_motion_attrib') }}
),

final AS (
    SELECT
        manual_trx_key,
        sales_order_line_key,
        dv_enterprise_inv_sku_id,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        dv_oa_flg,
        start_tv_dtm,
        end_tv_dtm,
        dv_source_type,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        sales_motion_timing_cd,
        sk_offer_attribution_id_int,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_gap_days,
        renewal_ref_rule
    FROM source_mt_sls_adj_sls_motion_attrib
)

SELECT * FROM final