{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_opex_headcount_forecast', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_OPEX_HEADCOUNT_FORECAST',
        'target_table': 'ST_OPEX_HEADCOUNT_FORECAST',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.941643+00:00'
    }
) }}

WITH 

source_ff_opex_headcount_forecast AS (
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
    FROM {{ source('raw', 'ff_opex_headcount_forecast') }}
),

transformed_exp_opex_headcount_forecast AS (
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
    scenario,
    1 AS batch_id,
    'I' AS action_code,
    CURRENT_TIMESTAMP() AS creation_date
    FROM source_ff_opex_headcount_forecast
),

final AS (
    SELECT
        batch_id,
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
        action_code,
        creation_date,
        scenario
    FROM transformed_exp_opex_headcount_forecast
)

SELECT * FROM final