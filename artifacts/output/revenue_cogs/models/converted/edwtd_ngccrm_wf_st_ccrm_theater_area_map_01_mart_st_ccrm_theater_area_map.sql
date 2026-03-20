{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_ccrm_theater_area_map', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_ST_CCRM_THEATER_AREA_MAP',
        'target_table': 'ST_CCRM_THEATER_AREA_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.889479+00:00'
    }
) }}

WITH 

source_ff_ccrm_theater_area_map AS (
    SELECT
        batch_id,
        theater_id,
        theater_name,
        area_id,
        area_name,
        sales_segment,
        create_datetime,
        action_code
    FROM {{ source('raw', 'ff_ccrm_theater_area_map') }}
),

final AS (
    SELECT
        batch_id,
        theater_id,
        theater_name,
        area_id,
        area_name,
        sales_segment,
        create_datetime,
        action_code
    FROM source_ff_ccrm_theater_area_map
)

SELECT * FROM final