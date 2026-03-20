{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_el_research_api_acts_f', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_EL_RESEARCH_API_ACTS_F',
        'target_table': 'EL_RESEARCH_API_ACTS_F',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:44.424632+00:00'
    }
) }}

WITH 

source_st_research_api_acts AS (
    SELECT
        bl_acts_api_log_key,
        acts_api_log_id,
        user_id,
        action,
        source,
        source_url,
        bk_service_request_num,
        start_time_dtm,
        end_time_dtm,
        mg_created_date,
        bl_creation_date,
        bl_created_by,
        bl_last_update_date,
        bl_last_updated_by,
        bl_resource_key,
        bl_workgroup_key,
        workgroup_id,
        workgroup_name,
        theater_name,
        delivery_channel_type,
        outsourcer_name,
        bl_load_id,
        edwsf_batch_id,
        edwsf_create_dtm,
        edwsf_create_user,
        edwsf_update_dtm,
        edwsf_update_user,
        edwsf_source_deleted_flag,
        edwsf_source_deleted_dtm
    FROM {{ source('raw', 'st_research_api_acts') }}
),

final AS (
    SELECT
        bl_acts_api_log_key,
        acts_api_log_id,
        user_id,
        action,
        source,
        source_url,
        bk_service_request_num,
        start_time_dtm,
        end_time_dtm,
        mg_created_date,
        bl_creation_date,
        bl_created_by,
        bl_last_update_date,
        bl_last_updated_by,
        bl_resource_key,
        bl_workgroup_key,
        workgroup_id,
        workgroup_name,
        theater_name,
        delivery_channel_type,
        outsourcer_name,
        bl_load_id,
        edwsf_batch_id,
        edwsf_create_dtm,
        edwsf_create_user,
        edwsf_update_dtm,
        edwsf_update_user,
        edwsf_source_deleted_flag,
        edwsf_source_deleted_dtm
    FROM source_st_research_api_acts
)

SELECT * FROM final