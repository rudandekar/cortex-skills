{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_w_fin_dfrd_rev_measure_hier', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_W_FIN_DFRD_REV_MEASURE_HIER',
        'target_table': 'W_FIN_DFRD_REV_MEASURE_HIER',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.712873+00:00'
    }
) }}

WITH 

source_w_fin_dfrd_rev_measure_hier AS (
    SELECT
        bk_dfrd_rev_msr_lvl_two_name,
        bk_service_flg,
        dfrd_rev_measure_lvl_one_name,
        balance_trend_rprt_flg,
        balance_waterfall_rprt_flg,
        bridge_rprt_flg,
        revenue_trend_rprt_flg,
        revenue_waterfall_rprt_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        product_subscription_flg,
        action_code,
        dml_type
    FROM {{ source('raw', 'w_fin_dfrd_rev_measure_hier') }}
),

final AS (
    SELECT
        bk_dfrd_rev_msr_lvl_two_name,
        bk_service_flg,
        dfrd_rev_measure_lvl_one_name,
        balance_trend_rprt_flg,
        balance_waterfall_rprt_flg,
        bridge_rprt_flg,
        revenue_trend_rprt_flg,
        revenue_waterfall_rprt_flg,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user,
        start_tv_dt,
        end_tv_dt,
        product_subscription_flg,
        action_code,
        dml_type
    FROM source_w_fin_dfrd_rev_measure_hier
)

SELECT * FROM final