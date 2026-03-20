{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_si_region', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_ST_SI_REGION',
        'target_table': 'ST_SI_REGION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.607988+00:00'
    }
) }}

WITH 

source_ff_si_region AS (
    SELECT
        batch_id,
        region_id,
        region_value,
        enabled_flag,
        region_desc,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_si_region') }}
),

final AS (
    SELECT
        batch_id,
        region_id,
        region_value,
        enabled_flag,
        region_desc,
        last_update_date,
        create_datetime,
        action_code
    FROM source_ff_si_region
)

SELECT * FROM final