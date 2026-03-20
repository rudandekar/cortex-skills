{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_st_wips_master_source', 'batch', 'edwtd_pos'],
    meta={
        'source_workflow': 'wf_m_ST_WIPS_MASTER_SOURCE',
        'target_table': 'ST_WIPS_MASTER_SOURCE',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:41:33.682502+00:00'
    }
) }}

WITH 

source_gg_wips_master_source AS (
    SELECT
        master_source_id,
        master_source_name,
        short_name,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'gg_wips_master_source') }}
),

final AS (
    SELECT
        master_source_id,
        master_source_name,
        short_name,
        active_flag,
        created_by,
        created_date,
        updated_by,
        last_update_date,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM source_gg_wips_master_source
)

SELECT * FROM final