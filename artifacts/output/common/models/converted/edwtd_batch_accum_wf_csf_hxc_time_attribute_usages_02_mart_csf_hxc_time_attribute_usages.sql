{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_hxc_time_attribute_usages', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_HXC_TIME_ATTRIBUTE_USAGES',
        'target_table': 'CSF_HXC_TIME_ATTRIBUTE_USAGES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.917551+00:00'
    }
) }}

WITH 

source_stg_csf_hxc_time_attribute_usages AS (
    SELECT
        time_attribute_usage_id,
        time_attribute_id,
        time_building_block_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        object_version_number,
        time_building_block_ovn,
        data_set_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_hxc_time_attribute_usages') }}
),

source_csf_hxc_time_attribute_usages AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        time_attribute_usage_id,
        time_attribute_id,
        time_building_block_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        object_version_number,
        time_building_block_ovn,
        data_set_id
    FROM {{ source('raw', 'csf_hxc_time_attribute_usages') }}
),

transformed_exp_csf_hxc_time_attribute_usages AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    time_attribute_usage_id,
    time_attribute_id,
    time_building_block_id,
    created_by,
    creation_date,
    last_updated_by,
    last_update_date,
    last_update_login,
    object_version_number,
    time_building_block_ovn,
    data_set_id
    FROM source_csf_hxc_time_attribute_usages
),

final AS (
    SELECT
        time_attribute_usage_id,
        time_attribute_id,
        time_building_block_id,
        created_by,
        creation_date,
        last_updated_by,
        last_update_date,
        last_update_login,
        object_version_number,
        time_building_block_ovn,
        data_set_id,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_hxc_time_attribute_usages
)

SELECT * FROM final