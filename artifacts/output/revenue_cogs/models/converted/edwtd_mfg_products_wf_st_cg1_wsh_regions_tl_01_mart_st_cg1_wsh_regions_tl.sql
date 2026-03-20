{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_cg1_wsh_regions_tl', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_ST_CG1_WSH_REGIONS_TL',
        'target_table': 'ST_CG1_WSH_REGIONS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.191629+00:00'
    }
) }}

WITH 

source_cg1_wsh_regions_tl AS (
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
    FROM {{ source('raw', 'cg1_wsh_regions_tl') }}
),

final AS (
    SELECT
        language_r,
        source_lang,
        region_id,
        continent,
        country,
        country_region,
        state,
        city,
        zone_r,
        postal_code_from,
        postal_code_to,
        alternate_name,
        county,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_cg1_wsh_regions_tl
)

SELECT * FROM final