{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_wg_l_object_type', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_FF_WG_L_OBJECT_TYPE',
        'target_table': 'FF_WG_L_OBJECT_TYPE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.182395+00:00'
    }
) }}

WITH 

source_wg_l_object_type AS (
    SELECT
        type_id,
        type_name,
        type_description
    FROM {{ source('raw', 'wg_l_object_type') }}
),

final AS (
    SELECT
        type_id,
        type_name,
        type_description
    FROM source_wg_l_object_type
)

SELECT * FROM final