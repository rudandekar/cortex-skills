{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_tss_resources_d_wtcalc', 'batch', 'edwtd_ca_services'],
    meta={
        'source_workflow': 'wf_m_EL_TSS_RESOURCES_D_WTCALC',
        'target_table': 'EL_TSS_RESOURCES_D',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.934787+00:00'
    }
) }}

WITH 

source_st_tss_resources_d_wtcalc AS (
    SELECT
        bl_resource_key,
        cco_id,
        cec_id,
        fnd_user_id,
        workgroup_id,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_active_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        bl_created_by,
        bl_creation_date,
        bl_last_updated_by,
        bl_last_update_date,
        bl_delete_flag,
        bl_load_id,
        resource_role_id
    FROM {{ source('raw', 'st_tss_resources_d_wtcalc') }}
),

transformed_expr AS (
    SELECT
    bl_resource_key,
    cco_id,
    cec_id,
    fnd_user_id,
    workgroup_id,
    bl_effective_from_date,
    bl_effective_to_date,
    bl_active_flag,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    bl_created_by,
    bl_creation_date,
    bl_last_updated_by,
    bl_last_update_date,
    bl_delete_flag,
    bl_load_id,
    resource_role_id
    FROM source_st_tss_resources_d_wtcalc
),

final AS (
    SELECT
        bl_resource_key,
        cco_id,
        cec_id,
        fnd_user_id,
        workgroup_id,
        bl_effective_from_date,
        bl_effective_to_date,
        bl_active_flag,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        bl_created_by,
        bl_creation_date,
        bl_last_updated_by,
        bl_last_update_date,
        bl_delete_flag,
        bl_load_id,
        resource_role_id
    FROM transformed_expr
)

SELECT * FROM final