{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_pl_fx_rate_publish', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_PL_FX_RATE_PUBLISH',
        'target_table': 'FF_PL_FX_RATE_PUBLISH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.607122+00:00'
    }
) }}

WITH 

source_pl_fx_rate_publish AS (
    SELECT
        fiscal_month_id,
        currency_code,
        conversion_rate,
        refresh_date
    FROM {{ source('raw', 'pl_fx_rate_publish') }}
),

transformed_exp_ff_pl_fx_rate_publish AS (
    SELECT
    fiscal_month_id,
    currency_code,
    conversion_rate,
    refresh_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_pl_fx_rate_publish
),

final AS (
    SELECT
        batch_id,
        fiscal_month_id,
        currency_code,
        conversion_rate,
        refresh_date,
        create_datetime,
        action_code
    FROM transformed_exp_ff_pl_fx_rate_publish
)

SELECT * FROM final