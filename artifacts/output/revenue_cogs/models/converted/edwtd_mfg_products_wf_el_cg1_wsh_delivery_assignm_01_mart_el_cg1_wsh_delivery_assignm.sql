{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_cg1_wsh_delivery_assignm', 'batch', 'edwtd_mfg_products'],
    meta={
        'source_workflow': 'wf_m_EL_CG1_WSH_DELIVERY_ASSIGNM',
        'target_table': 'EL_CG1_WSH_DELIVERY_ASSIGNM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.588161+00:00'
    }
) }}

WITH 

source_st_cg1_wsh_delivery_assignm AS (
    SELECT
        delivery_assignment_id,
        delivery_detail_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        delivery_id,
        parent_delivery_id,
        parent_delivery_detail_id,
        last_update_login,
        program_application_id,
        program_id,
        program_update_date,
        request_id,
        active_flag,
        wda_type,
        global_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'st_cg1_wsh_delivery_assignm') }}
),

final AS (
    SELECT
        delivery_assignment_id,
        delivery_detail_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        delivery_id,
        parent_delivery_id,
        parent_delivery_detail_id,
        last_update_login,
        program_application_id,
        program_id,
        program_update_date,
        request_id,
        active_flag,
        wda_type,
        global_name
    FROM source_st_cg1_wsh_delivery_assignm
)

SELECT * FROM final