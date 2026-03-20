{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_region', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_REGION',
        'target_table': 'FF_SI_REGION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.930178+00:00'
    }
) }}

WITH 

source_si_region AS (
    SELECT
        region_id,
        region_value,
        created_by,
        create_date,
        last_updated_by,
        last_update_date,
        enabled_flag,
        region_desc
    FROM {{ source('raw', 'si_region') }}
),

transformed_exp_si_region AS (
    SELECT
    region_id,
    region_value,
    last_update_date,
    enabled_flag,
    region_desc,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_si_region
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
    FROM transformed_exp_si_region
)

SELECT * FROM final