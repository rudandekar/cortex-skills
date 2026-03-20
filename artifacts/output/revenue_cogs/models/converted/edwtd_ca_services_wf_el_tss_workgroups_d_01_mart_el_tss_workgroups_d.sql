{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_workgroups_d', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_WORKGROUPS_D',
        'target_table': 'EL_TSS_WORKGROUPS_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.083046+00:00'
    }
) }}

WITH 

source_st_tss_workgroups_d AS (
    SELECT
        bl_workgroup_key,
        workgroup_id,
        workgroup_name,
        workgroup_desc,
        wkg_mgr_resource_id,
        wkg_mgr_first_name,
        wkg_mgr_last_name,
        wkg_mgr_email,
        sub_region_name,
        theater_name,
        delivery_channel_type,
        outsourcer_name,
        service_line_name,
        service_type,
        creation_date,
        last_update_date,
        bl_last_update_date,
        tech_id,
        active_flag,
        bl_active_flag,
        bl_delete_flag,
        bl_effective_to_date,
        bl_effective_from_date
    FROM {{ source('raw', 'st_tss_workgroups_d') }}
),

transformed_expr AS (
    SELECT
    bl_workgroup_key,
    workgroup_id,
    workgroup_name,
    workgroup_desc,
    wkg_mgr_resource_id,
    wkg_mgr_first_name,
    wkg_mgr_last_name,
    wkg_mgr_email,
    sub_region_name,
    theater_name,
    delivery_channel_type,
    outsourcer_name,
    service_line_name,
    service_type,
    creation_date,
    last_update_date,
    bl_last_update_date,
    tech_id,
    active_flag,
    bl_active_flag,
    bl_delete_flag,
    bl_effective_to_date,
    bl_effective_from_date
    FROM source_st_tss_workgroups_d
),

final AS (
    SELECT
        bl_workgroup_key,
        workgroup_id,
        workgroup_name,
        workgroup_desc,
        wkg_mgr_resource_id,
        wkg_mgr_first_name,
        wkg_mgr_last_name,
        wkg_mgr_email,
        sub_region_name,
        theater_name,
        delivery_channel_type,
        outsourcer_name,
        service_line_name,
        service_type,
        creation_date,
        last_update_date,
        bl_last_update_date,
        tech_id,
        active_flag,
        bl_active_flag,
        bl_delete_flag,
        bl_effective_to_date,
        bl_effective_from_date
    FROM transformed_expr
)

SELECT * FROM final