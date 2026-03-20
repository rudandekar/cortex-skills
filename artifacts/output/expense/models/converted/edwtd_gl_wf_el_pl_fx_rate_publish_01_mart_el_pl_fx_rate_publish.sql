{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_pl_fx_rate_publish', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_PL_FX_RATE_PUBLISH',
        'target_table': 'EL_PL_FX_RATE_PUBLISH',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.878404+00:00'
    }
) }}

WITH 

source_st_pl_fx_rate_publish AS (
    SELECT
        batch_id,
        fiscal_month_id,
        currency_code,
        conversion_rate,
        refresh_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_pl_fx_rate_publish') }}
),

final AS (
    SELECT
        fiscal_month_id,
        currency_code,
        conversion_rate,
        refresh_date
    FROM source_st_pl_fx_rate_publish
)

SELECT * FROM final