{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_ff_si_location_info', 'batch', 'edwtd_gl'],
    meta={
        'source_workflow': 'wf_m_FF_SI_LOCATION_INFO',
        'target_table': 'FF_SI_LOCATION_INFO',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:05:28.600668+00:00'
    }
) }}

WITH 

source_si_location_info AS (
    SELECT
        location_value,
        location_name,
        usage_description,
        company_value,
        start_date,
        end_date,
        create_date,
        created_by,
        last_update_date,
        last_updated_by,
        enabled_flag,
        info_published,
        erp_published,
        available_for_dept
    FROM {{ source('raw', 'si_location_info') }}
),

transformed_exp_si_location_info AS (
    SELECT
    location_value,
    location_name,
    company_value,
    last_update_date,
    'BatchId' AS batch_id,
    CURRENT_TIMESTAMP() AS create_datetime,
    'I' AS action_code
    FROM source_si_location_info
),

final AS (
    SELECT
        batch_id,
        location_value,
        location_name,
        company_value,
        last_update_date,
        create_datetime,
        action_code
    FROM transformed_exp_si_location_info
)

SELECT * FROM final