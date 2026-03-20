{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_si_functional_unit', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_EL_SI_FUNCTIONAL_UNIT',
        'target_table': 'EL_SI_FUNCTIONAL_UNIT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:29.147064+00:00'
    }
) }}

WITH 

source_st_si_functional_unit AS (
    SELECT
        batch_id,
        functional_unit_id,
        functional_unit_value,
        functional_unit_desc,
        enabled_flag,
        last_update_date,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_si_functional_unit') }}
),

final AS (
    SELECT
        functional_unit_id,
        functional_unit_value,
        functional_unit_desc,
        enabled_flag
    FROM source_st_si_functional_unit
)

SELECT * FROM final