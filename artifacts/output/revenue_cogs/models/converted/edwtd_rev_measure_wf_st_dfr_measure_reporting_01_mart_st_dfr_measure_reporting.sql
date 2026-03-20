{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_dfr_measure_reporting', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_DFR_MEASURE_REPORTING',
        'target_table': 'ST_DFR_MEASURE_REPORTING',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.404558+00:00'
    }
) }}

WITH 

source_ff_dfr_measure_reporting AS (
    SELECT
        measure_level_1,
        measure_level_2,
        service_flag,
        balance_trend_flag,
        balance_waterfall_flag,
        bridge_flag,
        revenue_trend_flag,
        revenue_waterfall_flag,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        prod_subscription_flg
    FROM {{ source('raw', 'ff_dfr_measure_reporting') }}
),

final AS (
    SELECT
        measure_level_1,
        measure_level_2,
        service_flag,
        balance_trend_flag,
        balance_waterfall_flag,
        bridge_flag,
        revenue_trend_flag,
        revenue_waterfall_flag,
        create_user,
        create_datetime,
        update_user,
        update_datetime,
        prod_subscription_flg
    FROM source_ff_dfr_measure_reporting
)

SELECT * FROM final