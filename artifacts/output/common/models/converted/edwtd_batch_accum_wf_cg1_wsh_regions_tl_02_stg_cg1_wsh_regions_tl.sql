{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_cg1_wsh_regions_tl', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_WSH_REGIONS_TL',
        'target_table': 'STG_CG1_WSH_REGIONS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.951370+00:00'
    }
) }}

WITH 

source_stg_cg1_wsh_regions_tl AS (
    SELECT
        language_code,
        source_lang,
        region_id,
        continent,
        country,
        country_region,
        state,
        city,
        zone_code,
        postal_code_from,
        postal_code_to,
        alternate_name,
        county,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_dml_type,
        trail_file_name,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_cg1_wsh_regions_tl') }}
),

source_cg1_wsh_regions_tl AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        language,
        source_lang,
        region_id,
        continent,
        country,
        country_region,
        state,
        city,
        zone,
        postal_code_from,
        postal_code_to,
        alternate_name,
        county,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login
    FROM {{ source('raw', 'cg1_wsh_regions_tl') }}
),

transformed_exp_cg1_wsh_regions_tl AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    language,
    source_lang,
    region_id,
    continent,
    country,
    country_region,
    state,
    city,
    zone,
    postal_code_from,
    postal_code_to,
    alternate_name,
    county,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login
    FROM source_cg1_wsh_regions_tl
),

final AS (
    SELECT
        language_code,
        source_lang,
        region_id,
        continent,
        country,
        country_region,
        state,
        city,
        zone_code,
        postal_code_from,
        postal_code_to,
        alternate_name,
        county,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        source_dml_type,
        trail_file_name,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_cg1_wsh_regions_tl
)

SELECT * FROM final