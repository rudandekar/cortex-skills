{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_tac_center_lines_d', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_TAC_CENTER_LINES_D',
        'target_table': 'EL_TSS_TAC_CENTER_LINES_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.610782+00:00'
    }
) }}

WITH 

source_st_tss_tac_center_lines_d AS (
    SELECT
        bl_tac_center_line_key,
        tac_center_line_id,
        tac_center_id,
        resource_site_id,
        workgroup_id,
        enabled_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        bl_active_flag,
        bl_delete_flag,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_creation_date,
        bl_created_by,
        bl_last_update_date,
        bl_last_updated_by,
        bl_source_system,
        bl_load_id
    FROM {{ source('raw', 'st_tss_tac_center_lines_d') }}
),

final AS (
    SELECT
        bl_tac_center_line_key,
        tac_center_line_id,
        tac_center_id,
        resource_site_id,
        workgroup_id,
        enabled_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        bl_active_flag,
        bl_delete_flag,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_creation_date,
        bl_created_by,
        bl_last_update_date,
        bl_last_updated_by,
        bl_source_system,
        bl_load_id,
        edw_create_user,
        edw_update_user,
        edw_create_datetime,
        edw_update_datetime
    FROM source_st_tss_tac_center_lines_d
)

SELECT * FROM final