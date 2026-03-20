{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_research_api_acts', 'batch', 'edwtd_rev_measure'],
    meta={
        'source_workflow': 'wf_m_ST_RESEARCH_API_ACTS',
        'target_table': 'ST_RESEARCH_API_ACTS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:33:45.189026+00:00'
    }
) }}

WITH 

source_ff_st_research_api_acts1 AS (
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
    FROM {{ source('raw', 'ff_st_research_api_acts1') }}
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
    FROM source_ff_st_research_api_acts1
)

SELECT * FROM final