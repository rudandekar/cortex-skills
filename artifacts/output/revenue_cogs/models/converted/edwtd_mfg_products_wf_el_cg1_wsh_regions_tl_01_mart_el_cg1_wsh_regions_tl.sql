{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cg1_wsh_regions_tl', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_EL_CG1_WSH_REGIONS_TL',
        'target_table': 'EL_CG1_WSH_REGIONS_TL',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.470949+00:00'
    }
) }}

WITH 

source_st_cg1_wsh_regions_tl AS (
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
    FROM {{ source('raw', 'st_cg1_wsh_regions_tl') }}
),

final AS (
    SELECT
        region_id,
        language_r,
        zone_r,
        country,
        global_name
    FROM source_st_cg1_wsh_regions_tl
)

SELECT * FROM final