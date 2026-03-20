{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_wg_l_work_group', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_WG_L_WORK_GROUP',
        'target_table': 'ST_WG_L_WORK_GROUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:46.609872+00:00'
    }
) }}

WITH 

source_ff_wg_l_work_group AS (
    SELECT
        object_number,
        natural_key,
        work_group_technology,
        delivery_channel,
        service_line,
        service_type,
        manager_name,
        manager_email,
        shift_times_gmt_start,
        shift_times_gmt_end,
        workgroup_shift,
        workgroup_site,
        workgroup_shiftcenter,
        hardware_nonhardware,
        funding_source,
        wg_technology_grouping,
        wg_technology,
        wg_engineer_type,
        wg_dl,
        wg_dl_master_theater,
        wg_delivery_leader,
        wg_delivery_leader_dept,
        wg_survey_type,
        wg_sampling_size
    FROM {{ source('raw', 'ff_wg_l_work_group') }}
),

transformed_expr AS (
    SELECT
    object_number,
    natural_key,
    work_group_technology,
    delivery_channel,
    service_line,
    service_type,
    manager_name,
    manager_email,
    shift_times_gmt_start,
    shift_times_gmt_end,
    workgroup_shift,
    workgroup_site,
    workgroup_shiftcenter,
    hardware_nonhardware,
    funding_source,
    wg_technology_grouping,
    wg_technology,
    wg_engineer_type,
    wg_dl,
    wg_dl_master_theater,
    wg_delivery_leader,
    wg_delivery_leader_dept,
    wg_survey_type,
    wg_sampling_size
    FROM source_ff_wg_l_work_group
),

final AS (
    SELECT
        object_number,
        natural_key,
        work_group_technology,
        delivery_channel,
        service_line,
        service_type,
        manager_name,
        manager_email,
        shift_times_gmt_start,
        shift_times_gmt_end,
        workgroup_shift,
        workgroup_site,
        workgroup_shiftcenter,
        hardware_nonhardware,
        funding_source,
        wg_technology_grouping,
        wg_technology,
        wg_engineer_type,
        wg_dl,
        wg_dl_master_theater,
        wg_delivery_leader,
        wg_delivery_leader_dept,
        wg_survey_type,
        wg_sampling_size
    FROM transformed_expr
)

SELECT * FROM final