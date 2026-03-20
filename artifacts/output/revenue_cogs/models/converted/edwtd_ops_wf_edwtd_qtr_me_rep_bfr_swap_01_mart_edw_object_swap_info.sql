{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_edwtd_replic_swap', 'batch', 'edwtd_ops'],
    meta={
        'source_workflow': 'wf_m_EDWTD_REPLIC_SWAP',
        'target_table': 'EDW_OBJECT_SWAP_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.419144+00:00'
    }
) }}

WITH 

source_edw_object_swap_info AS (
    SELECT
        job_group_id,
        rep_view,
        etl_view,
        temp_view,
        current_view,
        active_indicator,
        last_swap_date
    FROM {{ source('raw', 'edw_object_swap_info') }}
),

final AS (
    SELECT
        job_group_id,
        rep_view,
        etl_view,
        temp_view,
        current_view,
        active_indicator,
        last_swap_date
    FROM source_edw_object_swap_info
)

SELECT * FROM final