{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_lookups_d', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_LOOKUPS_D',
        'target_table': 'EL_TSS_LOOKUPS_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:43.989244+00:00'
    }
) }}

WITH 

source_st_tss_lookups_d AS (
    SELECT
        bl_lookup_key,
        lookup_id,
        source_table,
        lookup_type,
        code,
        name,
        description,
        start_date_active,
        end_date_active,
        bl_attribute1,
        bl_attribute2,
        bl_attribute3,
        bl_attribute4,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        bl_created_by,
        bl_creation_date,
        bl_last_updated_by,
        bl_last_update_date,
        bl_source_system,
        bl_delete_flag,
        bl_load_id,
        bl_attribute5,
        bl_attribute6,
        bl_attribute7,
        bl_attribute8,
        bl_attribute9,
        enabled_flag,
        attribute46,
        attribute47,
        problem_code_id
    FROM {{ source('raw', 'st_tss_lookups_d') }}
),

final AS (
    SELECT
        bl_lookup_key,
        lookup_id,
        source_table,
        lookup_type,
        code,
        name,
        description,
        start_date_active,
        end_date_active,
        bl_attribute1,
        bl_attribute2,
        bl_attribute3,
        bl_attribute4,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        bl_created_by,
        bl_creation_date,
        bl_last_updated_by,
        bl_last_update_date,
        bl_source_system,
        bl_delete_flag,
        bl_load_id,
        bl_attribute5,
        bl_attribute6,
        bl_attribute7,
        bl_attribute8,
        bl_attribute9,
        enabled_flag,
        attribute46,
        attribute47,
        problem_code_id,
        edw_create_dtm,
        edw_create_user,
        edw_update_dtm,
        edw_update_user
    FROM source_st_tss_lookups_d
)

SELECT * FROM final