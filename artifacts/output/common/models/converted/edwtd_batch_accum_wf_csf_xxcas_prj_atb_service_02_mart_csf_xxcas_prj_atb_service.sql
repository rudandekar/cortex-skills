{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_xxcas_prj_atb_service', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_ATB_SERVICE',
        'target_table': 'CSF_XXCAS_PRJ_ATB_SERVICE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.043334+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_prj_atb_service AS (
    SELECT
        atb_service_id,
        project_id,
        task_id,
        activity_id,
        resource_id,
        business_service_id,
        business_service_name,
        abt_type,
        created_date,
        created_by,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_atb_service') }}
),

source_csf_xxcas_prj_atb_service AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        atb_service_id,
        project_id,
        task_id,
        activity_id,
        resource_id,
        business_service_id,
        business_service_name,
        type,
        created_date,
        created_by
    FROM {{ source('raw', 'csf_xxcas_prj_atb_service') }}
),

transformed_exp_csf_xxcas_prj_atb_service AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    atb_service_id,
    project_id,
    task_id,
    activity_id,
    resource_id,
    business_service_id,
    business_service_name,
    type,
    created_date,
    created_by
    FROM source_csf_xxcas_prj_atb_service
),

final AS (
    SELECT
        atb_service_id,
        project_id,
        task_id,
        activity_id,
        resource_id,
        business_service_id,
        business_service_name,
        abt_type,
        created_date,
        created_by,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_atb_service
)

SELECT * FROM final