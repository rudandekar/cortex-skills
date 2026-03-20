{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_wsh_delivery_assignments', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_WSH_DELIVERY_ASSIGNMENTS',
        'target_table': 'CG1_WSH_DELIVERY_ASSIGNM',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.721680+00:00'
    }
) }}

WITH 

source_cg1_wsh_delivery_assignments AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        delivery_assignment_id,
        delivery_id,
        parent_delivery_id,
        delivery_detail_id,
        parent_delivery_detail_id,
        creation_date,
        created_by,
        last_update_date,
        last_updated_by,
        last_update_login,
        program_application_id,
        program_id,
        program_update_date,
        request_id,
        active_flag,
        type
    FROM {{ source('raw', 'cg1_wsh_delivery_assignments') }}
),

source_stg_cg1_wsh_delivery_assignm AS (
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
        source_dml_type,
        source_commit_time,
        trail_file_name,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_wsh_delivery_assignm') }}
),

transformed_exp_cg1_wsh_delivery_assignments AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    delivery_assignment_id,
    delivery_id,
    parent_delivery_id,
    delivery_detail_id,
    parent_delivery_detail_id,
    creation_date,
    created_by,
    last_update_date,
    last_updated_by,
    last_update_login,
    program_application_id,
    program_id,
    program_update_date,
    request_id,
    active_flag,
    type
    FROM source_stg_cg1_wsh_delivery_assignm
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
        source_dml_type,
        source_commit_time,
        trail_file_name,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_wsh_delivery_assignments
)

SELECT * FROM final