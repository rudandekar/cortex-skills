{{ config(
    materialized='table',
    schema='',
    tags=['wf_m_cg1_oe_reasons', 'batch', 'edwtd_batch_accum'],
    meta={
        'source_workflow': 'wf_m_CG1_OE_REASONS',
        'target_table': 'CG1_OE_REASONS',
        'generated_by': 'INFA2DBT_accelerator_v2.0.0',
        'generation_timestamp': '2026-03-19T18:22:00.795571+00:00'
    }
) }}

WITH 

source_cg1_oe_reasons AS (
    SELECT
        source_dml_type,
        fully_qualified_table_name,
        source_commit_time,
        refresh_datetime,
        trail_position,
        token,
        refresh_day,
        reason_id,
        header_id,
        entity_id,
        entity_code,
        version_number,
        reason_type,
        reason_code,
        comments,
        creation_date,
        created_by,
        last_updated_by,
        last_update_date,
        last_update_login,
        context,
        attribute1,
        attribute2,
        attribute3,
        attribute4,
        attribute5,
        attribute6,
        attribute7,
        attribute8,
        attribute9,
        attribute10,
        attribute11,
        attribute12,
        attribute13,
        attribute14,
        attribute15,
        attribute16,
        attribute17,
        attribute18,
        attribute19,
        attribute20,
        line_id
    FROM {{ source('raw', 'cg1_oe_reasons') }}
),

source_stg_cg1_oe_reasons AS (
    SELECT
        global_name,
        reason_id,
        header_id,
        entity_id,
        entity_code,
        reason_type,
        reason_code,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM {{ source('raw', 'stg_cg1_oe_reasons') }}
),

transformed_exp_cg1_oe_reasons AS (
    SELECT
    source_dml_type,
    source_commit_time,
    refresh_datetime,
    reason_id,
    header_id,
    entity_id,
    entity_code,
    reason_type,
    reason_code
    FROM source_stg_cg1_oe_reasons
),

final AS (
    SELECT
        global_name,
        reason_id,
        header_id,
        entity_id,
        entity_code,
        reason_type,
        reason_code,
        trail_file_name,
        source_dml_type,
        source_commit_time,
        refresh_datetime,
        processed_flag
    FROM transformed_exp_cg1_oe_reasons
)

SELECT * FROM final