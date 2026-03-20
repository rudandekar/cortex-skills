{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_mt_pos_rtnr_smc_allocation', 'batch', 'edwtd_sales_orders'],
    meta={
        'source_workflow': 'wf_m_MT_POS_RTNR_SMC_ALLOCATION',
        'target_table': 'MT_POS_RTNR_SMC_ALLOCATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.977891+00:00'
    }
) }}

WITH 

source_mt_pos_rtnr_smc_allocation AS (
    SELECT
        bk_pos_transaction_id_int,
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
        sales_order_line_key,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_gap_days,
        renewal_ref_rule
    FROM {{ source('raw', 'mt_pos_rtnr_smc_allocation') }}
),

source_wi_n_pos_trx_ln_as_new_or_rnwl_incr AS (
    SELECT
        bk_pos_transaction_id_int,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        start_tv_dtm,
        end_tv_dtm,
        sales_motion_timing_cd,
        sales_order_line_key,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_gap_days,
        renewal_ref_rule
    FROM {{ source('raw', 'wi_n_pos_trx_ln_as_new_or_rnwl_incr') }}
),

source_wi_pos_rtnr_smc_allocation AS (
    SELECT
        bk_pos_transaction_id_int,
        sales_motion_cd,
        dv_allocation_pct,
        dv_service_category_cd,
        start_tv_dtm,
        end_tv_dtm,
        sales_motion_timing_cd,
        sales_order_line_key,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_gap_days,
        renewal_ref_rule
    FROM {{ source('raw', 'wi_pos_rtnr_smc_allocation') }}
),

final AS (
    SELECT
        bk_pos_transaction_id_int,
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
        sales_order_line_key,
        renewal_ref_id,
        renewal_ref_cd,
        renewal_gap_days,
        renewal_ref_rule
    FROM source_wi_pos_rtnr_smc_allocation
)

SELECT * FROM final