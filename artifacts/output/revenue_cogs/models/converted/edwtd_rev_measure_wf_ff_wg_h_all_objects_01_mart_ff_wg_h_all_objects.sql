{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_wg_h_all_objects', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_FF_WG_H_ALL_OBJECTS',
        'target_table': 'FF_WG_H_ALL_OBJECTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.398714+00:00'
    }
) }}

WITH 

source_wg_h_all_objects AS (
    SELECT
        object_number,
        object_name,
        object_type_id,
        create_date,
        natural_key,
        issynthetic,
        isprocessed,
        isactive,
        lastvaliddate,
        isdisabled
    FROM {{ source('raw', 'wg_h_all_objects') }}
),

final AS (
    SELECT
        object_number,
        object_name,
        object_type_id,
        create_date,
        natural_key,
        issynthetic,
        isprocessed,
        isactive,
        lastvaliddate,
        isdisabled
    FROM source_wg_h_all_objects
)

SELECT * FROM final