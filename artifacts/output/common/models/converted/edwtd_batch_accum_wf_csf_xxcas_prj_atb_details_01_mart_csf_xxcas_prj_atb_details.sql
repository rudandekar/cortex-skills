{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_xxcas_prj_atb_details', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_XXCAS_PRJ_ATB_DETAILS',
        'target_table': 'CSF_XXCAS_PRJ_ATB_DETAILS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.862294+00:00'
    }
) }}

WITH 

source_stg_csf_xxcas_prj_atb_details AS (
    SELECT
        atb_dtl_id,
        project_id,
        task_id,
        resource_id,
        business_service,
        architecture,
        technology,
        sub_technology,
        prcnt,
        creation_date,
        created_by,
        update_date,
        updated_by,
        source,
        type1,
        orig_percent,
        architecture_id,
        technology_id,
        business_id,
        sub_technology_id,
        user_weightage,
        activity_id,
        ogg_key_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_xxcas_prj_atb_details') }}
),

source_csf_xxcas_prj_atb_details AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        atb_dtl_id,
        project_id,
        task_id,
        resource_id,
        business_service,
        architecture,
        technology,
        sub_technology,
        percent,
        creation_date,
        created_by,
        update_date,
        updated_by,
        source,
        type,
        orig_percent,
        architecture_id,
        technology_id,
        business_id,
        sub_technology_id,
        user_weightage,
        activity_id,
        ogg_key_id
    FROM {{ source('raw', 'csf_xxcas_prj_atb_details') }}
),

transformed_exp_csf_xxcas_prj_atb_details AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    atb_dtl_id,
    project_id,
    task_id,
    resource_id,
    business_service,
    architecture,
    technology,
    sub_technology,
    percent,
    creation_date,
    created_by,
    update_date,
    updated_by,
    source,
    type,
    orig_percent,
    architecture_id,
    technology_id,
    business_id,
    sub_technology_id,
    user_weightage,
    activity_id,
    ogg_key_id
    FROM source_csf_xxcas_prj_atb_details
),

final AS (
    SELECT
        atb_dtl_id,
        project_id,
        task_id,
        resource_id,
        business_service,
        architecture,
        technology,
        sub_technology,
        prcnt,
        creation_date,
        created_by,
        update_date,
        updated_by,
        source,
        type1,
        orig_percent,
        architecture_id,
        technology_id,
        business_id,
        sub_technology_id,
        user_weightage,
        activity_id,
        ogg_key_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_xxcas_prj_atb_details
)

SELECT * FROM final