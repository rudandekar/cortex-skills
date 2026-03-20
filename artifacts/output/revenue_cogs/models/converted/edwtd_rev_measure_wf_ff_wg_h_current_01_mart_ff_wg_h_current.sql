{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_wg_h_current', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_FF_WG_H_CURRENT',
        'target_table': 'FF_WG_H_CURRENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.019343+00:00'
    }
) }}

WITH 

source_wg_h_current AS (
    SELECT
        hierarchy_id,
        object_number,
        reports_to_object_number,
        hierarchy_level,
        hierarchy_version,
        create_date,
        hierarchy_string
    FROM {{ source('raw', 'wg_h_current') }}
),

final AS (
    SELECT
        hierarchy_id,
        object_number,
        reports_to_object_number,
        hierarchy_level,
        hierarchy_version,
        create_date,
        hierarchy_string
    FROM source_wg_h_current
)

SELECT * FROM final