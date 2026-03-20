{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cg1_wsh_zone_regions', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_EL_CG1_WSH_ZONE_REGIONS',
        'target_table': 'EL_CG1_WSH_ZONE_REGIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.496523+00:00'
    }
) }}

WITH 

source_st_cg1_wsh_zone_regions AS (
    SELECT
        zone_region_id,
        region_id,
        parent_region_id,
        party_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        zone_flag,
        postal_code_from,
        postal_code_to,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_cg1_wsh_zone_regions') }}
),

final AS (
    SELECT
        region_id,
        parent_region_id,
        global_name
    FROM source_st_cg1_wsh_zone_regions
)

SELECT * FROM final