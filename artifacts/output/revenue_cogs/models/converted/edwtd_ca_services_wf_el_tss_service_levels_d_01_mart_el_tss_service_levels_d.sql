{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_service_levels_d', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_SERVICE_LEVELS_D',
        'target_table': 'EL_TSS_SERVICE_LEVELS_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.085898+00:00'
    }
) }}

WITH 

source_st_tss_service_levels_d AS (
    SELECT
        bl_service_level_key,
        service_level_id,
        service_group_code,
        service_group_meaning,
        service_level_code,
        service_level_group_desc,
        service_level_desc,
        business_process_id,
        business_process_name,
        business_process_desc,
        reaction_hour_number,
        reaction_hour_desc,
        svo_reaction_hour_number,
        service_region_id,
        active_start_date,
        active_end_date,
        record_type,
        enabled_flag,
        created_by,
        creation_date,
        last_update_by,
        last_update_date,
        bl_created_by,
        bl_creation_date,
        bl_last_updated_by,
        bl_last_update_date,
        bl_source_system,
        bl_load_id
    FROM {{ source('raw', 'st_tss_service_levels_d') }}
),

final AS (
    SELECT
        bl_service_level_key,
        service_level_id,
        service_group_code,
        service_group_meaning,
        service_level_code,
        service_level_group_desc,
        service_level_desc,
        business_process_id,
        business_process_name,
        business_process_desc,
        reaction_hour_number,
        reaction_hour_desc,
        svo_reaction_hour_number,
        service_region_id,
        active_start_date,
        active_end_date,
        record_type,
        enabled_flag,
        created_by,
        creation_date,
        last_update_by,
        last_update_date,
        bl_created_by,
        bl_creation_date,
        bl_last_updated_by,
        bl_last_update_date,
        bl_source_system,
        bl_load_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_tss_service_levels_d
)

SELECT * FROM final