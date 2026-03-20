{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_ego_fnd_dsc_flx_ctx_ext', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_EGO_FND_DSC_FLX_CTX_EXT',
        'target_table': 'CSF_EGO_FND_DSC_FLX_CTX_EXT',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:01.091754+00:00'
    }
) }}

WITH 

source_stg_csf_ego_fnd_dsc_flx_ctx_ext AS (
    SELECT
        attr_group_id,
        application_id,
        descriptive_flexfield_name,
        descriptive_flex_context_code,
        multi_row,
        security_type,
        owning_party_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        view_privilege_id,
        edit_privilege_id,
        agv_name,
        region_code,
        business_event_flag,
        pre_business_event_flag,
        variant,
        num_of_rows,
        num_of_cols,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_ego_fnd_dsc_flx_ctx_ext') }}
),

source_csf_ego_fnd_dsc_flx_ctx_ext AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        attr_group_id,
        application_id,
        descriptive_flexfield_name,
        descriptive_flex_context_code,
        multi_row,
        security_type,
        owning_party_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        view_privilege_id,
        edit_privilege_id,
        agv_name,
        region_code,
        business_event_flag,
        pre_business_event_flag,
        variant,
        num_of_rows,
        num_of_cols,
        zd_edition_name,
        zd_sync
    FROM {{ source('raw', 'csf_ego_fnd_dsc_flx_ctx_ext') }}
),

transformed_exp_csf_ego_fnd_dsc_flx_ctx_ext AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    attr_group_id,
    application_id,
    descriptive_flexfield_name,
    descriptive_flex_context_code,
    multi_row,
    security_type,
    owning_party_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    view_privilege_id,
    edit_privilege_id,
    agv_name,
    region_code,
    business_event_flag,
    pre_business_event_flag,
    variant,
    num_of_rows,
    num_of_cols,
    zd_edition_name,
    zd_sync
    FROM source_csf_ego_fnd_dsc_flx_ctx_ext
),

final AS (
    SELECT
        attr_group_id,
        application_id,
        descriptive_flexfield_name,
        descriptive_flex_context_code,
        multi_row,
        security_type,
        owning_party_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        view_privilege_id,
        edit_privilege_id,
        agv_name,
        region_code,
        business_event_flag,
        pre_business_event_flag,
        variant,
        num_of_rows,
        num_of_cols,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_ego_fnd_dsc_flx_ctx_ext
)

SELECT * FROM final