{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_wg_h_current', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_WG_H_CURRENT',
        'target_table': 'ST_WG_H_CURRENT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.466138+00:00'
    }
) }}

WITH 

source_ff_wg_h_current AS (
    SELECT
        hierarchy_id,
        object_number,
        reports_to_object_number,
        hierarchy_level,
        hierarchy_version,
        create_date,
        hierarchy_string
    FROM {{ source('raw', 'ff_wg_h_current') }}
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
    FROM source_ff_wg_h_current
)

SELECT * FROM final