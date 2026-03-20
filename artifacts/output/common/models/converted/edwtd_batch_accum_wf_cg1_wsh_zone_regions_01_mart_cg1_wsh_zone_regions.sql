{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_wsh_zone_regions', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_WSH_ZONE_REGIONS',
        'target_table': 'CG1_WSH_ZONE_REGIONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.544552+00:00'
    }
) }}

WITH 

source_cg1_wsh_zone_regions AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
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
        postal_code_to
    FROM {{ source('raw', 'cg1_wsh_zone_regions') }}
),

source_stg_cg1_wsh_zone_regions AS (
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
        source_dml_type,
        trail_file_name,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_cg1_wsh_zone_regions') }}
),

transformed_exp_cg1_wsh_zone_regions AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
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
    postal_code_to
    FROM source_stg_cg1_wsh_zone_regions
),

final AS (
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
        source_dml_type,
        trail_file_name,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_cg1_wsh_zone_regions
)

SELECT * FROM final