{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_vw_ldsg_extract', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_VW_LDSG_EXTRACT',
        'target_table': 'FF_VW_LDSG_EXTRACT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.874537+00:00'
    }
) }}

WITH 

source_vw_ldsg_extract AS (
    SELECT
        spend_category,
        fiscal_month_name,
        node_level05_value,
        forecast_amt,
        load_date
    FROM {{ source('raw', 'vw_ldsg_extract') }}
),

final AS (
    SELECT
        spend_category,
        fiscal_month_name,
        node_level05_value,
        forecast_amt,
        load_date
    FROM source_vw_ldsg_extract
)

SELECT * FROM final