{{ config(
    materialized='view',
    schema='',
    tags=['wf_m_dwh_stg_code_translation', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_DWH_STG_CODE_TRANSLATION',
        'target_table': 'ST_DWH_STG_CODE_TRANSLATION',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.806229+00:00'
    }
) }}

WITH 

source_dwh_stg_code_translation AS (
    SELECT
        table_name,
        column_name,
        code,
        string_name,
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day
    FROM {{ source('raw', 'dwh_stg_code_translation') }}
),

source_st_dwh_stg_code_translation AS (
    SELECT
        table_name,
        column_name,
        code,
        string_name,
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day
    FROM {{ source('raw', 'st_dwh_stg_code_translation') }}
),

transformed_ex_dwh_stg_code_translation AS (
    SELECT
    table_name,
    column_name,
    code,
    string_name,
    source_dml_type,
    fully_qualified_table_name,
    source_commit_time,
    refresh_datetime,
    trail_position,
    token,
    refresh_day
    FROM source_st_dwh_stg_code_translation
),

final AS (
    SELECT
        table_name,
        column_name,
        code,
        string_name,
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day
    FROM transformed_ex_dwh_stg_code_translation
)

SELECT * FROM final