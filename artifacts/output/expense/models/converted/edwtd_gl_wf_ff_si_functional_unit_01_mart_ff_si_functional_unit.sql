{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_functional_unit', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_FUNCTIONAL_UNIT',
        'target_table': 'FF_SI_FUNCTIONAL_UNIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.113632+00:00'
    }
) }}

WITH 

source_si_functional_unit AS (
    SELECT
        functional_unit_id,
        functional_unit_value,
        functional_unit_desc,
        create_date,
        created_by,
        last_update_date,
        last_updated_by,
        enabled_flag
    FROM {{ source('raw', 'si_functional_unit') }}
),

transformed_exp_si_functional_unit AS (
    SELECT
    functional_unit_id,
    functional_unit_value,
    functional_unit_desc,
    create_date,
    created_by,
    last_update_date,
    last_updated_by,
    enabled_flag,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_si_functional_unit
),

final AS (
    SELECT
        batch_id,
        functional_unit_id,
        functional_unit_value,
        functional_unit_desc,
        enabled_flag,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_si_functional_unit
)

SELECT * FROM final