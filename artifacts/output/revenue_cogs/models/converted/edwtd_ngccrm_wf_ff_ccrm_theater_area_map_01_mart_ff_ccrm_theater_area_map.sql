{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_ccrm_theater_area_map', 'batch', 'edwtd_ngccrm'],
    meta={
        'source_workflow': 'wf_m_FF_CCRM_THEATER_AREA_MAP',
        'target_table': 'FF_CCRM_THEATER_AREA_MAP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.227516+00:00'
    }
) }}

WITH 

source_ccrm_theater_area_map AS (
    SELECT
        theater_id,
        theater_name,
        area_id,
        area_name,
        sales_segment
    FROM {{ source('raw', 'ccrm_theater_area_map') }}
),

transformed_exp_ff_ccrm_theater_area_map AS (
    SELECT
    theater_id,
    theater_name,
    area_id,
    area_name,
    sales_segment,
    'Batch_Id' AS batch_id_out,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_ccrm_theater_area_map
),

final AS (
    SELECT
        batch_id,
        theater_id,
        theater_name,
        area_id,
        area_name,
        sales_segment,
        craete_datetime,
        action_code
    FROM transformed_exp_ff_ccrm_theater_area_map
)

SELECT * FROM final