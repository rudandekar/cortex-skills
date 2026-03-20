{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_csf_pa_processes', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CSF_PA_WF_PROCESSES',
        'target_table': 'CSF_PA_WF_PROCESSES',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.938832+00:00'
    }
) }}

WITH 

source_csf_pa_wf_processes AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        wf_type_code,
        item_type,
        item_key,
        entity_key1,
        entity_key2,
        description,
        last_updated_by,
        last_update_date,
        creation_date,
        created_by,
        last_update_login
    FROM {{ source('raw', 'csf_pa_wf_processes') }}
),

source_stg_csf_pa_wf_processes AS (
    SELECT
        wf_type_code,
        item_type,
        item_key,
        entity_key1,
        entity_key2,
        description,
        last_updated_by,
        last_update_date,
        creation_date,
        created_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM {{ source('raw', 'stg_csf_pa_wf_processes') }}
),

transformed_exp_csf_pa_wf_processes AS (
    SELECT
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day,
    wf_type_code,
    item_type,
    item_key,
    entity_key1,
    entity_key2,
    description,
    last_updated_by,
    last_update_date,
    creation_date,
    created_by,
    last_update_login,
    v_MAX_SOURCE_COMMIT_TIME AS v_max_source_commit_time
    FROM source_stg_csf_pa_wf_processes
),

final AS (
    SELECT
        wf_type_code,
        item_type,
        item_key,
        entity_key1,
        entity_key2,
        description,
        last_updated_by,
        last_update_date,
        creation_date,
        created_by,
        last_update_login,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime
    FROM transformed_exp_csf_pa_wf_processes
)

SELECT * FROM final