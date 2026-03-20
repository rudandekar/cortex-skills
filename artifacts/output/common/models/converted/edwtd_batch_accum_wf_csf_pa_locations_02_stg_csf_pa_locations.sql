{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_csf_pa_locations', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_LOCATIONS',
        'target_table': 'STG_CSF_PA_LOCATIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.601020+00:00'
    }
) }}

WITH 

source_csf_pa_locations AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        location_id,
        city,
        region,
        country_code,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login
    FROM {{ source('raw', 'csf_pa_locations') }}
),

source_stg_csf_pa_locations AS (
    SELECT
        location_id,
        city,
        region,
        country_code,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_locations') }}
),

transformed_exp_csf_pa_locations AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    location_id,
    city,
    region,
    country_code,
    last_update_date,
    last_updated_by,
    creation_date,
    created_by,
    last_update_login
    FROM source_stg_csf_pa_locations
),

final AS (
    SELECT
        location_id,
        city,
        region,
        country_code,
        last_update_date,
        last_updated_by,
        creation_date,
        created_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_locations
)

SELECT * FROM final