{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_dmp_co_return_lookup', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_DMP_CO_RETURN_LOOKUP',
        'target_table': 'DMP_CO_RETURN_LOOKUP',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.808327+00:00'
    }
) }}

WITH 

source_dmp_co_return_lookup AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        lookup_id,
        lookup_index,
        lookup_type,
        lookup_value,
        lookup_subtype,
        lookup_sub_category,
        lookup_order,
        active,
        created_by,
        created_on,
        updated_by,
        updated_on
    FROM {{ source('raw', 'dmp_co_return_lookup') }}
),

source_stg_dmp_co_return_lookup AS (
    SELECT
        lookup_id,
        lookup_index,
        lookup_type,
        lookup_value,
        lookup_subtype,
        lookup_sub_category,
        lookup_order,
        active,
        created_by,
        created_on,
        updated_by,
        updated_on,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_dmp_co_return_lookup') }}
),

transformed_exp_dmp_co_return_lookup AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    lookup_id,
    lookup_index,
    lookup_type,
    lookup_value,
    lookup_subtype,
    lookup_sub_category,
    lookup_order,
    active,
    created_by,
    created_on,
    updated_by,
    updated_on
    FROM source_stg_dmp_co_return_lookup
),

final AS (
    SELECT
        lookup_id,
        lookup_index,
        lookup_type,
        lookup_value,
        lookup_subtype,
        lookup_sub_category,
        lookup_order,
        active,
        created_by,
        created_on,
        updated_by,
        updated_on,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_dmp_co_return_lookup
)

SELECT * FROM final