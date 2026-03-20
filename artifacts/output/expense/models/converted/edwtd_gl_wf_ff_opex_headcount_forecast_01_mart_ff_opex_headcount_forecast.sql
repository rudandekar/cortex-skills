{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_opex_headcount_forecast', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_OPEX_HEADCOUNT_FORECAST',
        'target_table': 'FF_OPEX_HEADCOUNT_FORECAST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.874750+00:00'
    }
) }}

WITH 

source_hc_forecast_opx AS (
    SELECT
        processed_fiscal_month,
        fiscal_month,
        dept_id,
        regfte_fcst_hc,
        temp_fcst_hc,
        int_fcst_hc,
        attr_fcst_hc,
        newhire_fcst_hc,
        active_flag,
        refresh_date,
        scenario
    FROM {{ source('raw', 'hc_forecast_opx') }}
),

final AS (
    SELECT
        processed_fiscal_month,
        fiscal_month,
        dept_id,
        regfte_fcst_hc,
        temp_fcst_hc,
        int_fcst_hc,
        attr_fcst_hc,
        newhire_fcst_hc,
        active_flag,
        refresh_date,
        scenario
    FROM source_hc_forecast_opx
)

SELECT * FROM final