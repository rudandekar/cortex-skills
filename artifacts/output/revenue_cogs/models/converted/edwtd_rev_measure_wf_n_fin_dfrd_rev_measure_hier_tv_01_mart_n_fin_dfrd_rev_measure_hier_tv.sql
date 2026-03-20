{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_n_fin_dfrd_rev_measure_hier_tv', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_N_FIN_DFRD_REV_MEASURE_HIER_TV',
        'target_table': 'N_FIN_DFRD_REV_MEASURE_HIER_TV',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.682626+00:00'
    }
) }}

WITH 

source_n_fin_dfrd_rev_measure_hier AS (
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
        product_subscription_flg
    FROM {{ source('raw', 'n_fin_dfrd_rev_measure_hier') }}
),

source_n_fin_dfrd_rev_measure_hier_tv AS (
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
        product_subscription_flg
    FROM {{ source('raw', 'n_fin_dfrd_rev_measure_hier_tv') }}
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
        product_subscription_flg
    FROM source_n_fin_dfrd_rev_measure_hier_tv
)

SELECT * FROM final