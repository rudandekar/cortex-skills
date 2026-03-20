{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_ccrm_theater_area_map', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_EL_CCRM_THEATER_AREA_MAP',
        'target_table': 'EL_CCRM_THEATER_AREA_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.617878+00:00'
    }
) }}

WITH 

source_st_ccrm_theater_area_map AS (
    SELECT
        batch_id,
        theater_id,
        theater_name,
        area_id,
        area_name,
        sales_segment,
        create_datetime,
        action_code
    FROM {{ source('raw', 'st_ccrm_theater_area_map') }}
),

final AS (
    SELECT
        theater_id,
        theater_name,
        area_id,
        area_name,
        sales_segment,
        created_by,
        creation_date,
        last_updated_by,
        last_updated_date
    FROM source_st_ccrm_theater_area_map
)

SELECT * FROM final