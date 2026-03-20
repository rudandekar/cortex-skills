{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_pl_fx_rate_publish', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_PL_FX_RATE_PUBLISH',
        'target_table': 'ST_PL_FX_RATE_PUBLISH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.752992+00:00'
    }
) }}

WITH 

source_ff_pl_fx_rate_publish AS (
    SELECT
        batch_id,
        fiscal_month_id,
        currency_code,
        conversion_rate,
        refresh_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_pl_fx_rate_publish') }}
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
    FROM source_ff_pl_fx_rate_publish
)

SELECT * FROM final